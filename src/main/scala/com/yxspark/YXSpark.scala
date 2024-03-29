package com.yxspark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.conf._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, split}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.expressions.Base64
import org.codehaus.jackson.map.ext.CoreXMLDeserializers.GregorianCalendarDeserializer
import java.util.GregorianCalendar

case class SadaRecord(scrip:String, ad:String, ts:String, url:String, ref:String, ua:String, dstip:String,cookie:String,
                      srcPort:String)
object YXSpark {
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  // change hdfspath to private/db/ for test
  val hdfspath: String = "hdfs://ns1/user/" + System.getProperty("user.name") + "/private/db/"
  val hdfsconfpath: String = hdfspath + "all/config/"
  val today: String = sdf.format(new Date())

  val spark: SparkSession = SparkSession.builder().appName("YXSpark").getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.sqlContext.implicits._
  val mode: String = sc.getConf.get("spark.submit.deployMode")

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  val sadaRecordArr = Array("scrip", "ad", "ts", "url", "ref", "ua", "dstip","cookie", "srcPort")
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString().replace("\"", "")
  }

  def tblexists(tbl: String): Boolean = {
    fs.exists(new org.apache.hadoop.fs.Path(tbl))
  }

  def getsources(client: String) : List[List[String]] = {
    val lclient = client.toLowerCase
    val configfile = lclient + ".config"

    val configpath = if (mode == "cluster") hdfsconfpath
    else if (mode == "client")  ""

    val sources :List[List[String]] =
      if (mode == "cluster") {
        sc.textFile(configpath + configfile).map(l=>l.split(" +").toList).collect()
          .filter(l => l.head.toLowerCase() == "source").toList
      } else if (mode == "client") {
        scala.io.Source.fromFile(configpath + configfile).getLines
          .map(l => l.split(" +").toList)
          .filter(l => l.head.toLowerCase() == "source")
          .toList
      } else {
        List[List[String]]()
      }
    sources
  }

  def showcounts(tbl: String): Unit = {
    if (tblexists(tbl)) {
      val df = sc.textFile(tbl).toDF()
      println(tbl + " has " + df.count + " rows")
    }
  }

  def getvars(client: String, sdate: String) : Map[String, String] = {
    val lclient = client.toLowerCase
    val configfile = lclient + ".config"

    val configpath = if (mode == "cluster") hdfsconfpath
    else if (mode == "client")  ""

    val configm :Map[String, String] =
      if (mode == "cluster") {
        sc.textFile(configpath + configfile).map({ l =>
          val t = l.split(" +")
          (t(0).toLowerCase(), t(1))
        }).collect().toMap
      } else if (mode == "client") {
        scala.io.Source.fromFile(configpath + configfile).getLines.map({ l =>
          val t = l.split(" +")
          (t(0).toLowerCase(), t(1))
        }).toMap
      } else {
        Map[String, String]()
      }

    val normalrun = if (sdate.contains(",") || sdate.contains("*")) false else true

    val dir = if (normalrun) "/daily/" + sdate + "/"
    else "/manual/" + sdate.replace("{", "").replace("}", "").replace(",", "_").replace("*", "_") + "/"

    val outputdlm = if (configm.contains("output_dlm")) configm("output_dlm") else "\t"
    val tagdlm = if (configm.contains("tag_dlm")) configm("tag_dlm") else "$$$"
    val hdfspublic = "hdfs://ns1/user/gdpi/public/"
    val history_dir = if (configm("history_dir").endsWith("/")) configm("history_dir") else configm("history_dir") + "/"

    val varsmap = Map(
      ("adcookie", hdfspublic + "sada_gdpi_adcookie/" + sdate + "/*/*.gz"),
      ("newclick", hdfspublic + "sada_new_click/" + sdate + "/*/*.gz"),
      ("post", hdfspublic + "sada_gdpi_post_click/" + sdate + "/*/*.gz"),
      ("adcookie_s0", hdfspath + "all" + dir + "adcookie_s0"),
      ("post_s0", hdfspath + "all" + dir + "post_s0"),
      ("newclick_s0", hdfspath + "all" + dir + "newclick_s0"),
      ("adcookie_s1", hdfspath + lclient + dir + "adcookie_s1"),
      ("post_s1", hdfspath + lclient + dir + "post_s1"),
      ("newclick_s1", hdfspath + lclient + dir + "newclick_s1"),
      ("stg_s2", hdfspath + lclient + dir + "stg_s2"),
      ("stg_acc_s3", hdfspath + lclient + dir + "stg_acc_s3"),
      ("stg_fuz_s3", hdfspath + lclient + dir + "stg_fuz_s3"),
      ("final_tbl", hdfspath + lclient + dir + "final_tbl"),
      ("kv_tbl", hdfspath + lclient + dir + "kv_tbl"),
      ("kv_enc_tbl", hdfspath + lclient + dir + "kv_enc_tbl"),
      ("history_dir", hdfspath + history_dir),
      ("history_tbl", hdfspath + history_dir + "*"),
      ("appname_file", configpath + configm("appname_file")),
      ("tag_file", configpath + configm("tag_file")),
      ("output_dlm", outputdlm),
      ("tag_dlm", tagdlm)
    )

    varsmap
  }

  def getfiles(filename: String) : List[String] = {
    val fs = FileSystem.get(new java.net.URI(hdfsconfpath),new Configuration())
    val files = fs.listStatus(new Path(hdfsconfpath))
    val ds = fs.getFileStatus(new Path(hdfsconfpath))
    if(ds.isDirectory()){
      files.filter(n=>n.isFile && n.getPath.toString.contains(filename)).map({n=>n.getPath.toString}).toList
    }else{
      List[String]()
    }

    //    val d = new File(".")
    //    if (d.exists && d.isDirectory) {
    //      d.listFiles.filter(n => n.isFile && n.getName.contains(filename))
    //        .map(n => n.getName).toList
    //    } else {
    //      List[String]()
    //    }
  }

  /**
    *
    * @param varsmap
    * @param sources
    * @param client
    * @param sdate
    * @param searchPattern: url match pattern, only with url filed ("url") or with url and ref ("full"  -- now word other than ulr is ok)
    * @param savePattern: save pattern, only save ad,ua, first 50 characters of url ("simple"), or all fields ("full" -- now work other than full is ok)
    */
  def stg_s0(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, searchPattern:String , savePattern:String ): Unit = {
    println("Running stg_s0")
//    val calendar = Calendar.getInstance()
//    calendar.add(Calendar.DATE, -1)
//    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
    val sdateFormat = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyyMMdd").parse(sdate))
    //assert(searchPattern == "full" || searchPattern == "url")
    //assert(savePattern == "full" || searchPattern == "simple")

    sources.foreach({l =>
      println("Searching " + varsmap(l(1)))

      val files = getfiles(l(2).replace("*", ""))
      val allkeywords = files
        .map({f =>
          println("Reading search keywords in " + f)
          //          scala.io.Source.fromFile(f).getLines.map(k => k.split(" +")).toList

          // add source file to it
          sc.textFile(f).map(l=> l.split(" +") ++ Array(new Path(f).getName.split("\\.").dropRight(1).mkString("."))).collect().toList
        }).flatten

      println("Total search keywords: " + allkeywords.length)

      // get source tag, normal prior than pv.
      val addSource = udf{(url:String, ref:String) =>
        val keywords = allkeywords.filter(l => l.dropRight(1).forall(url.contains(_)) || l.dropRight(1).forall(ref.contains(_))).
          map(arr => arr.last).distinct

        keywords.mkString(",")
        //val firstKey = keywords.sorted.head  // for the case of dropWhile drop all elements
        //if  (keywords.size == 1) firstKey else keywords.dropWhile(e => e.contains("pv")).headOption.getOrElse(firstKey)
      }
      val toString = udf{binaryArray:Array[Byte] =>
        new String(binaryArray)
      }
      // should unbase in postgresql
      val sourceTbl = sc.textFile(varsmap(l(1))).map{
        line =>
          val arr:Array[String] = line.split("\t")
          val ref = if (arr(4).toLowerCase == "nodef") "" else arr(4)
          val ua = if ( arr(5).toLowerCase  == "nodef") "" else arr(5)
          val cookie = if (arr(7).toLowerCase == "nodef") "" else arr(7)
          (arr(0),arr(1),arr(2),arr(3),ref,ua,arr(6),cookie,arr(8))
      }.toDF(sadaRecordArr:_*).
      withColumn("url", regexp_replace($"url","\\p{C}|\\\\.","?")).
      withColumn("ref",regexp_replace(decode(unbase64($"ref"),"UTF-8"),"\\p{C}|\\\\.","?"))
      //withColumn("ua",decode(unbase64($"ua"),"UTF-8")).
      .withColumn("cookie",regexp_replace(decode(unbase64($"cookie"),"UTF-8"),"\\p{C}|\\\\.","?"))
      //.withColumn("cookie",toString(unbase64($"cookie")))
      .as[SadaRecord]


      val tbl  =
        if (searchPattern == "url" ){
          val saveTbl = sourceTbl.filter{
              record =>
                // caution: exclude last element i.e. src tag
                allkeywords.exists(l => l.dropRight(1).forall(record.url.contains(_)))
            }.toDF(sadaRecordArr:_*)
            .withColumn("src", addSource($"url",lit(""))).withColumn("dt",lit(sdateFormat)).withColumn("category",lit(l(1))) // add meta information
          if (savePattern == "full") saveTbl else saveTbl.select($"ad",$"ua",substring($"url",0,50),$"src",$"dt")

        }else {
          val saveTbl = sourceTbl.filter{
            record =>
              allkeywords.exists(l => l.dropRight(1).forall(record.url.contains(_) )|| l.dropRight(1).forall(record.ref.contains(_)) )
          }.toDF(sadaRecordArr:_*)
            .withColumn("src",addSource($"url",$"ref")).withColumn("dt", lit(sdateFormat)).withColumn("category",lit(l(1))) // add meta information
          if (savePattern == "full") saveTbl else saveTbl.select($"ad",$"ua",substring($"url",0,50),substring($"cookie",0,50),$"src",$"dt")
        }


      tbl.coalesce(1000).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm"))
        .save(varsmap(l(1) + "_s0"))

      showcounts(varsmap(l(1) + "_s0"))
    })
  }

  def stg_s1(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, source: String = "") {
    println("Running stg_s1")

    sources.foreach({l =>
      val dfname = varsmap(l(1) + source)
      println("Searching " + dfname)
      //      val keywords = scala.io.Source.fromFile(l(2)).getLines.map(k => k.split(" +")).toList
      val keywords = sc.textFile(hdfsconfpath + l(2)).map(l=>l.split(" +")).collect().toList

      val tbl = sc.textFile(dfname)
        .filter(line => keywords.exists(k => k.forall(line.contains(_)))).toDF

      tbl.coalesce(500).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm")).save(varsmap(l(1) + "_s1"))

      showcounts(varsmap(l(1) + "_s1"))
    })
  }

  def stg_s2(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String) {
    println("Running stg_s2")

    val data = sources.map({l =>
      println("Reading " + varsmap(l(1) + "_s1"))
      sc.textFile(varsmap(l(1) + "_s1"))
        .toDF.withColumn("_tmp", split($"value", varsmap("output_dlm")))
        .select(
          $"_tmp".getItem(l(3).toInt).as("ad"),
          $"_tmp".getItem(l(4).toInt).as("ua"),
          $"_tmp".getItem(l(5).toInt).as("url")
        ).drop($"_tmp").withColumn("data_source", lit(l(1)))
    })

    //    val al = scala.io.Source.fromFile(varsmap("appname_file")).getLines.map(l => l.split(" +")).toList
    val al = sc.textFile(varsmap("appname_file")).map(l=>l.split(" +")).collect().toList

    //al.foreach(l => println(l.mkString(",")))
    val ltagdlm = varsmap("tag_dlm")

    val tbl = data.reduce(_ union _)
      .filter("ad != 'none' and ad != ''")
      .map({r =>
        val appname = al
          .filter(x => r(2).toString.contains(x(0)) || x(0) == "*")
          .map(x => x(1)).headOption.getOrElse("")

        (r(0).toString, r(1).toString, r(3).toString + ltagdlm + appname, appname)
      }).filter(r => r._4 != "").drop($"_4")
      .dropDuplicates

    tbl.coalesce(50).write.format("com.databricks.spark.csv")
      .option("delimiter", varsmap("output_dlm")).save(varsmap("stg_s2"))

    showcounts(varsmap("stg_s2"))
  }

  def stg_s3_comb(varsmap:Map[String,String],af:Int): Unit ={
    val ldlm = varsmap("output_dlm")
    val ht = if(af == 0) varsmap("stg_acc_s3") else if (af == 1) varsmap("stg_fuz_s3") + "_s1" else ""

    val df = sc.textFile(ht+ "_*")
    df.saveAsTextFile(ht)

    if (af == 1) {
      val stg_acc_s3 = sc.textFile(varsmap("stg_acc_s3")).toDF
        .withColumn("_tmp", split($"value", ldlm))
        .select(
          $"_tmp".getItem(0).as("mobile")
        ).drop($"_tmp").dropDuplicates().toDF()

      val stg_fuz_s3_s1 = sc.textFile(ht).toDF
        .withColumn("_tmp", split($"value", ldlm))
        .select(
          $"_tmp".getItem(0).as("mobile"),
          $"_tmp".getItem(1).as("pattern")
        ).drop($"_tmp").dropDuplicates().toDF()

      val stg_fuz_s3 = stg_fuz_s3_s1.join(stg_acc_s3, Seq("mobile"), "leftanti")

      stg_fuz_s3.coalesce(10).write.format("com.databricks.spark.csv")
        .option("delimiter", ldlm).save(varsmap("stg_fuz_s3"))

      showcounts(varsmap("stg_fuz_s3"))
    }
  }

  def stg_s3(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, af: Int, pieceAmount:Int = 10000) {
    println("Running stg_s3")

    val ht :String = if(af == 0) varsmap("stg_acc_s3") else if (af == 1) varsmap("stg_fuz_s3") + "_s1" else ""
    val ldlm = varsmap("output_dlm")

    if (mode == "cluster") {
      import hlwbbigdata.phone

      val inputtbl = sc.textFile(varsmap("stg_s2")).map({r =>
        val arr = r.split(ldlm)
        (arr(0), arr(1), arr(2))
      })
      val counts = (inputtbl.count()/pieceAmount).toInt
      val pieces = inputtbl.randomSplit(Array.fill(counts)(1))
//      var outputtbl = phone.phone_match(spark,pieces(0),af.toString)
//      for (piece <- pieces.drop(1))
//        outputtbl = outputtbl.union(phone.phone_match(spark, inputtbl, af.toString))

      pieces.zipWithIndex.foreach{
        rddPair =>
        val piece = phone.phone_match(spark, rddPair._1, af.toString)
        piece.write.format("com.databricks.spark.csv").option("delimiter","\t").
          save(ht+ "_" + rddPair._2)

      }

    } else if (mode == "client") {
      import sys.process._

      val shell = if (varsmap("output_dlm") == "\t") "spark-submit-hlwbbigdata-tab.sh" else ""

      val cmd = "./" + shell + " " + varsmap("stg_s2") + " " + ht + " " + af

      println("Running" + cmd)
      val ret = cmd.!

      println("Process finished with exit code " + ret)

      if (ret != 0) {
        println("Shell job returned error code, job aborted")
        System.exit(1)
      }
    }

    showcounts(ht.toString)

  }

  def final_tbl(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String) {
    val ltagdlm = varsmap("tag_dlm")

    println("Running final_tbl")

    val data = Seq(("acc", varsmap("stg_acc_s3")), ("fuz", varsmap("stg_fuz_s3")))
      .filter(x => tblexists(x._2)).map({x =>
      sc.textFile(x._2).toDF
        .withColumn("_tmp", split($"value", varsmap("output_dlm")))
        .select(
          $"_tmp".getItem(0).as("id"),
          $"_tmp".getItem(1).as("_stmp")
        ).drop($"_tmp").withColumn("pattern", concat($"_stmp", lit(ltagdlm + x._1)))
        .drop($"_stmp")
    })

//    val history_tbl = sc.textFile(varsmap("history_tbl")).toDF
//      .withColumn("_tmp", split($"value", varsmap("output_dlm")))
//      .select(
//        $"_tmp".getItem(0).as("id")
//      ).drop($"_tmp")
    //    val tagl = scala.io.Source.fromFile(varsmap("tag_file")).getLines.map(l => l.split(" +")).toList
    val tagl = sc.textFile(varsmap("tag_file")).map(l=>l.split(" +")).collect().toList
    //al.foreach(l => println(l.mkString(",")))

    // judgement if history directory exists.
    val dropTbl = if (fs.exists(new Path(varsmap("history_tbl").dropRight(2))))
      data.reduce(_ union _).
        join(sc.textFile(varsmap("history_tbl")).map(row => row.split(varsmap("output_dlm"))(0)).toDF("id"), Seq("id"), "leftanti")
    else data.reduce(_ union _)



    val tbl = dropTbl
      .map({r =>
        val tag = tagl.filter({x =>
          val tags = x(0).replace(ltagdlm, " ").split(" +").toList
          tags.forall(r(1).toString.contains(_))
        }).map(x => (x(1), if (x.length == 3) x(2) else ""))
          .headOption.getOrElse(("", ""))
        (r(0).toString, r(1).toString, tag._1, tag._2)
      })
      .filter(x => x._3 != "").dropDuplicates

    tbl.coalesce(10).write.format("com.databricks.spark.csv")
      .option("delimiter", varsmap("output_dlm")).save(varsmap("final_tbl"))

    showcounts(varsmap("final_tbl"))
  }

  def history_tbl(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, tag: String) {
    val ldlm = varsmap("output_dlm")

    println("Running history_tbl")

    val ht = if (tag == "all") varsmap("kv_tbl") + "*/*" else varsmap("kv_tbl") + "_" + tag

    val df = sc.textFile(ht).map({r =>
      val strarr = r.split(ldlm)
      if (strarr.length == 4) strarr(3).replace("\"", "") else ""
    }).filter(x => x != "").toDF().dropDuplicates()

    val tblp = varsmap("history_dir") + "history_tbl_" + tag + "_" + sdate

    df.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", varsmap("output_dlm"))
      .save(tblp)

    showcounts(tblp)
  }

  def kv_tbl(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, tag: String, batch: Int) {
    val ldlm = varsmap("output_dlm")
    val ltagdlm = varsmap("tag_dlm")

    println("Running kv_tbl")

    val df = sc.textFile(varsmap("final_tbl")).toDF
      .withColumn("_tmp", split($"value", varsmap("output_dlm")))
      .select(
        $"_tmp".getItem(0).as("mobile"),
        $"_tmp".getItem(1).as("pattern"),
        $"_tmp".getItem(2).as("tag_prefix"),
        $"_tmp".getItem(3).as("inj_value")
      ).drop($"_tmp").dropDuplicates().toDF()

    val df2 = if (tag == "all") df else df.filter("tag_prefix = '" + tag + "'")

    val counts = df2.groupBy("tag_prefix").count().collect()

    counts.zipWithIndex.foreach({r =>
      println("Tag " + r._1.get(0) + " has " + r._1.get(1) + " rows")

      val key = r._1.get(0).toString + "_" + "%02d".format(r._2 + batch) + "_" + today

      val df3 = df2.filter("tag_prefix = '" + r._1.get(0).toString + "'")

      val tbl = df3.rdd.zipWithIndex().map({x =>
        val pattern = x._1.get(1).toString.replace(ltagdlm, " ").split(" +")
        val injval = if (x._1.get(3) != "") ":" + x._1.get(3) else ""
        (key + "_" + x._2, "ad" + ldlm + pattern(1) + ":" + key + injval + ldlm + x._1.get(0).toString)
      }).union(sc.parallelize(Seq((key + "_total", r._1.get(1).toString))))
        .toDF()

      tbl.coalesce(1).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm")).save(varsmap("kv_tbl") + "_" + key)

      showcounts(varsmap("kv_tbl") + "_" + key)
    })
  }

  def kv_enc_tbl(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, pfx: String, source: String = "_s1") {
    println("Running kv_enc_tbl")

    val data = sources.filter(l => tblexists(varsmap(l(1) + source))).map({l =>
      println("Reading " + varsmap(l(1) + source))

      sc.textFile(varsmap(l(1) + source))
        .toDF.withColumn("data_source", lit(l(1)))
    })

    val key = pfx + "_" + sdf.format(new Date()) + "_"
    val enc = new Enc()
    val ldlm = varsmap("output_dlm")

    val df = data.reduce(_ union _).rdd.zipWithIndex
      .map({r =>
        (key + r._2, enc.encrypt(r._1.get(0).toString + ldlm + r._1.get(1).toString))
      }).toDF

    val rv = (key + "total", enc.encrypt(df.count.toString))
    val nr = sc.parallelize(Seq(rv)).toDF
    val tbl = df.union(nr)

    tbl.coalesce(30).write.format("com.databricks.spark.csv")
      .option("delimiter", varsmap("output_dlm")).save(varsmap("kv_enc_tbl"))

    showcounts(varsmap("kv_enc_tbl"))
  }

  //  def local_file_enc(hdfstbl: String, file: String, pfx: String) {
  //    val strl = scala.io.Source.fromFile(file).getLines.toList
  //
  //    val key = pfx + "_" + sdf.format(new Date()) + "_"
  //    val enc = new Enc()
  //    val ldlm = varsmap("output_dlm")
  //
  //    val df = strl.zipWithIndex
  //      .map({r =>
  //        (key + r._2, enc.encrypt(r._1))
  //      }).toDF
  //
  //    val rv = (key + "total", enc.encrypt(df.count.toString))
  //    val nr = sc.parallelize(Seq(rv)).toDF
  //    df.union(nr)
  //      .coalesce(1).write.format("com.databricks.spark.csv")
  //      .option("delimiter", varsmap("output_dlm")).save(hdfstbl)
  //  }

  def showdf(hdfstbl: String) {
    val df = sc.textFile(hdfstbl).toDF
    println(df.count)
    df.show
  }

  def run_all(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, tag:String, batch: Int) {
    stg_s1(varsmap, sources, client, sdate)
    stg_s2(varsmap, sources, client, sdate)
    stg_s3(varsmap, sources, client, sdate, 0);stg_s3_comb(varsmap,0)
    stg_s3(varsmap, sources, client, sdate, 1);stg_s3_comb(varsmap,1)
    final_tbl(varsmap, sources, client, sdate)
    kv_tbl(varsmap, sources, client, sdate, tag, batch)
  }

  def run_all_with_stg_s0(varsmap: Map[String, String], sources: List[List[String]], client: String, sdate: String, tag:String, batch: Int) {
    stg_s1(varsmap, sources, client, sdate, "_s0")
    stg_s2(varsmap, sources, client, sdate)
    stg_s3(varsmap, sources, client, sdate, 0);stg_s3_comb(varsmap,0)
    stg_s3(varsmap, sources, client, sdate, 1);stg_s3_comb(varsmap,1)
    final_tbl(varsmap, sources, client, sdate)
    kv_tbl(varsmap, sources, client, sdate, tag, batch)
  }

  def main(args: Array[String]) {
    val prog = args(0)
    val client = if (prog == "stg_s0" || prog == "kv_enc_tbl_with_stg_s0") "all" else args(1)
    val sdate = args(2)
    val pieceAmount = args.lift(3).getOrElse("10000")

    val varsmap = getvars(client, sdate)
    val sources = getsources(client)

    prog match {
      case "stg_s0" => stg_s0(varsmap, sources, client, sdate, args.lift(3).getOrElse("url"), args.lift(4).getOrElse("full"))
      case "stg_s1" => stg_s1(varsmap, sources, client, sdate)
      case "stg_s1_with_stg_s0" => stg_s1(varsmap, sources, client, sdate, "_s0")
      case "stg_s2" => stg_s2(varsmap, sources, client, sdate)
      case "stg_s3_acc" => stg_s3(varsmap, sources, client, sdate, 0,pieceAmount.toInt);stg_s3_comb(varsmap,0)
      case "stg_s3_fuz" => stg_s3(varsmap, sources, client, sdate, 1,pieceAmount.toInt);stg_s3_comb(varsmap,1)
      case "final_tbl" => final_tbl(varsmap, sources, client, sdate)
      case "kv_tbl" => kv_tbl(varsmap, sources, client, sdate, args(3), args(4).toInt)
      case "history_tbl" => history_tbl(varsmap, sources, client, sdate, args(3))
      case "kv_enc_tbl" => kv_enc_tbl(varsmap, sources, client, sdate, args(3))
      case "kv_enc_tbl_with_stg_s0" => kv_enc_tbl(varsmap, sources, client, sdate, args(3), "_s0")
      case "run_all" => run_all(varsmap, sources, client, sdate, args(3), args(4).toInt)
      case "run_all_with_stg_s0" => run_all_with_stg_s0(varsmap, sources, client, sdate, args(3), args(4).toInt)
      case "cdr" => cdr(client, sdate, args(3))
    }
  }




  private def inOrOut(prjName:String,batchId:String):String={
    val map = (prjName,batchId) match {
      case ("daoxila","22") => "call_in"
      case ("daoxila","23") => "call_out"
      case ("qijia","ZT1") => "call_in"
      case ("qijia","ZT2") => "call_out"
      case ("dk","ZT1") => "call_in"
      case ("dk","ZT2") => "call_out"
      case ("yypx","ZT1") => "call_in"
      case ("yypx","ZT2") => "call_out"
      case ("cfapx","ZT1") => "call_in"
      case ("cfapx","ZT2") => "call_out"

    }
    map
  }

  def dropHistory(prjName:String, tagName:String,srcPath:String, destPath:String, historyPath:String) {
    val tagStrigSuf = prjName match {
      case "daoxila" => ":96928575"
      case "futures" => ":96928579"
      case _ => ""
    }

    val tagString = ":" + tagName + "_" + today + tagStrigSuf

    val subIndex = if (prjName == "daoxila") 5 else 4

    val history_tbl = sc.textFile(historyPath + "/*").map(row => row.split("\t")(0)).
      toDF("mobile")

    val df = sc.textFile(srcPath).map({row =>
      val arr = row.split("\t")

      (arr(0), arr(1).substring(0, subIndex) + tagString)
    }).toDF("mobile", "tag")
      .join(history_tbl, Seq("mobile"), "leftanti")
      .dropDuplicates(Seq("mobile"))

    df.coalesce(10).write.format("com.databricks.spark.csv").option("delimiter","\t").save(destPath)
  }

  def filterCdr(srcPath:String,configPath:String,filterPath:String, remainPath:String): Unit ={
    val tagFile = sc.textFile(configPath).map(row => row.split(" ")).collect().toList
    val srcDF = sc.textFile(srcPath)

    val filterDF = srcDF.filter(row => tagFile.exists((l:Array[String]) => row.contains(l(0))))
    val remainDF = srcDF.filter(row => !tagFile.exists((l:Array[String]) => row.contains(l(0))))

    filterDF.saveAsTextFile(filterPath)
    remainDF.saveAsTextFile(remainPath)
  }

  def kvMatch(srcPath:String, tagName:String,  destPath:String) {
    val inDF = sc.textFile(srcPath).collect()

    //inDF.toDF().show()
    //println("count is " + inDF.count())

    val kvDF = inDF.zipWithIndex.map({row =>
      val arr = row._1.toString.split("\t")
      val batchid = if (tagName.contains("DXL_15")) {
        val i = arr(1).split(":")
        i(1) = tagName+"_"+today
        i.mkString(":")
      }else arr(1)

      (tagName + "_" + today + "_" + row._2, "ad" + "\t" + batchid + "\t" + arr(0))

    }).:+(tagName+"_" + today +"_total", inDF.size.toString)


    //kvDF.show
    sc.parallelize(kvDF.toSeq).toDF()
      .coalesce(1).write.format("com.databricks.spark.csv").option("delimiter","\t").save(destPath)
  }

  // save mobile (method dropHistory ) to history
  def historyTbl(srcPath:String, savePath:String) {
    val mobileDF = sc.textFile(srcPath).map(row => row.split("\t")(0)).toDF("mobile")

    mobileDF.coalesce(1).write.format("com.databricks.spark.csv").
      option("delimiter","\t").save(savePath)
  }

  def cdr(prjName:String, sdate: String, tagName:String) {
    // prjname: daoxila , qijia, futures
    // tagName: DXL_22_01, DLX_23_02, QJW_ZT1_01, si_t_1 etc.

    val inOrOutString = inOrOut(prjName,tagName.split("_")(1))

    // set path

    // history path
    val historyDir = "hdfs://ns1/user/u_tel_hlwb_xgq/private/lxc_xgq/" + prjName + "_final_history"
    val saveHistoryDir = historyDir + "/" + prjName + "_cdr_" + inOrOutString + "_" + sdate

    // normal path
    val configPath = s"hdfs://ns1/user/u_tel_hlwb_xgq/private/cdr/config/${prjName}_cdr_tag.txt"


    val hadoopDir = "hdfs://ns1/user/u_tel_hlwb_xgq/private/cdr/" + prjName + "/" + sdate


    val hadoopPath = hadoopDir + "/" + prjName + "_" + inOrOutString + "_" + sdate
    // if file name is not empty, then put the file up instead of default
    val filterPath = hadoopDir + "/" + prjName + "_" + inOrOutString + "_filter_" + sdate
    val remainPath = hadoopDir + "/" + prjName + "_" + inOrOutString + "_remain_" + sdate
    val dropHistoryPath = hadoopDir + "/" + prjName + "_drop_history_" + inOrOutString + "_" + sdate
    val kvPath = hadoopDir + "/" + prjName + "_kv_" + inOrOutString + "_" + sdate
    val remainKvPath = hadoopDir + "/" + prjName + "_remain_kv_" + inOrOutString + "_" + sdate
    val filterKvPath = hadoopDir + "/" + prjName + "_filter_kv_" + inOrOutString + "_" + sdate

    // deleting files
    //deleteFile(hadoopPath)
    //deleteFile(dropHistoryPath)
    //deleteFile(kvPath)
    //deleteFile(saveHistoryDir)


    // run stages
    //fromLocalToHadoop(localPath, hadoopPath)
    dropHistory(prjName, tagName,hadoopPath,dropHistoryPath,historyDir)

    if (prjName == "daoxila"){
      filterCdr(dropHistoryPath,configPath,filterPath,remainPath)
      kvMatch(remainPath,tagName,remainKvPath)

      if (inOrOutString == "call_in")
        kvMatch(filterPath,"DXL_15_03",filterKvPath)
      else
        kvMatch(filterPath,"DXL_15_04",filterKvPath)

    }else{
      kvMatch(dropHistoryPath,tagName,kvPath)

    }
    historyTbl(dropHistoryPath,saveHistoryDir)

  }
}