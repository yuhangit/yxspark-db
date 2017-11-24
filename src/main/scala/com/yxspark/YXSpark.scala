package com.yxspark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit, split}

object YXSpark {
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val hdfspath: String = "hdfs://ns1/user/" + System.getProperty("user.name") + "/private/"
  val hdfsconfpath: String = "hdfs://ns1/user/" + System.getProperty("user.name") + "/private/all/config/"
  val today: String = sdf.format(new Date())

  val spark: SparkSession = SparkSession.builder().appName("YXSpark").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString().replace("\"", "")
  }

  def tblexists(tbl: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    fs.exists(new org.apache.hadoop.fs.Path(tbl))
  }

  def getsources(client: String) : List[List[String]] = {
    val lclient = client.toLowerCase
    val configfile = lclient + ".config"

    //    val sources = scala.io.Source.fromFile(configfile).getLines
    //      .map(l => l.split(" +").toList)
    //      .filter(l => l.head.toLowerCase() == "source")
    //      .toList
    val sources = sc.textFile(hdfsconfpath + configfile).map(l=>l.split(" +").toList).collect()
      .filter(l => l.head.toLowerCase() == "source").toList

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

    //    val configm = scala.io.Source.fromFile(configfile).getLines.map({l =>
    //      val t = l.split(" +")
    //      (t(0).toLowerCase(), t(1))
    //    }).toMap
    val configm = sc.textFile(hdfsconfpath + configfile).map({l=>
      val t = l.split(" +")
      (t(0).toLowerCase(), t(1))
    }).collect().toMap

    val normalrun = if (sdate.contains(",") || sdate.contains("*")) false else true

    val dir = if (normalrun) "/daily/" + sdate + "/"
    else "/manual/" + sdate.replace("{", "").replace("}", "").replace(",", "_").replace("*", "_") + "/"

    val outputdlm = if (configm.contains("output_dlm")) configm("output_dlm") else "\t"
    val tagdlm = if (configm.contains("tag_dlm")) configm("tag_dlm") else "$$$"
    val hdfspublic = "hdfs://ns1/user/gdpi/public/"

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
      ("history_tbl", hdfspath + configm("history_tbl")),
      ("appname_file", hdfsconfpath + configm("appname_file")),
      ("tag_file", hdfsconfpath + configm("tag_file")),
      ("output_dlm", outputdlm),
      ("tag_dlm", tagdlm)
    )

    varsmap
  }

  def getfiles(filename: String) : List[String] = {

    //    val d = new File(".")
    //    if (d.exists && d.isDirectory) {
    //      d.listFiles.filter(n => n.isFile && n.getName.contains(filename))
    //        .map(n => n.getName).toList
    //    } else {
    //      List[String]()
    //    }
    val fs = FileSystem.get(new java.net.URI("/user/u_tel_hlwb_xgq/private/all/config/"),new Configuration())
    val files = fs.listStatus(new Path("/user/u_tel_hlwb_xgq/private/all/config/"))
    val ds = fs.getFileStatus(new Path("/user/u_tel_hlwb_xgq/private/all/config/"))
    if(ds.isDirectory()){
      files.filter(n=>n.isFile && n.getPath.toString.contains(filename)).map({n=>n.getPath.toString}).toList
    }else{
      List[String]()
    }
  }

  def stg_s0(sdate: String): Unit = {
    val client = "all"
    val varsmap = getvars(client, sdate)
    val sources = getsources(client)

    println("Running stg_s0")

    sources.foreach({l =>
      println("Searching " + varsmap(l(1)))

      val files = getfiles(l(2).replace("*", ""))
      val allkeywords = files
        .map({f =>
          println("Reading search keywords in " + f)
          //          scala.io.Source.fromFile(f).getLines.map(k => k.split(" +")).toList
          sc.textFile(f).map(l=>l.split(" +")).collect().toList
        }).flatten

      println("Total search keywords: " + allkeywords.length)

      val tbl = sc.textFile(varsmap(l(1)))
        .filter(line => allkeywords.exists(k => k.forall(line.contains(_)))).toDF

      tbl.coalesce(1000).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm"))
        .save(varsmap(l(1) + "_s0"))

      showcounts(varsmap(l(1) + "_s0"))
    })
  }

  def stg_s1(client: String, sdate: String, source: String = "") {
    val varsmap = getvars(client, sdate)
    val sources = getsources(client)

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

  def stg_s2(client: String, sdate: String) {
    val varsmap = getvars(client, sdate)
    val sources = getsources(client)

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

  def stg_s3(client: String, sdate: String, af: Int) {
    import sys.process._

    val varsmap = getvars(client, sdate)

    println("Running stg_s3")

    val ht = if(af == 0) varsmap("stg_acc_s3") else if (af == 1) varsmap("stg_fuz_s3") + "_s1"
    val shell = if (varsmap("output_dlm") == "\t") "spark-submit-hlwbbigdata-tab.sh" else ""

    val cmd = "./" + shell + " " + varsmap("stg_s2") + " " + ht + " " + af
    //val cmd = "hadoop fs -cat "+ hdfsconfpath + shell + " | exec sh -s " + varsmap("stg_s2") + " " + ht + " " + af

    println("Running" + cmd)

    val ret = cmd.!


    if (af == 1) {
      val stg_acc_s3 = sc.textFile(varsmap("stg_acc_s3")).toDF
        .withColumn("_tmp", split($"value", varsmap("output_dlm")))
        .select(
          $"_tmp".getItem(0).as("mobile"),
          $"_tmp".getItem(2).as("pattern")
        ).drop($"_tmp").dropDuplicates().toDF()

      val stg_fuz_s3_s1 = sc.textFile(varsmap("stg_fuz_s3") + "_s1").toDF
        .withColumn("_tmp", split($"value", varsmap("output_dlm")))
        .select(
          $"_tmp".getItem(0).as("mobile"),
          $"_tmp".getItem(2).as("pattern")
        ).drop($"_tmp").dropDuplicates().toDF()

      val stg_fuz_s3 = stg_fuz_s3_s1.join(stg_acc_s3, Seq("mobile"), "leftanti")

      stg_fuz_s3.coalesce(10).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm")).save(varsmap("stg_fuz_s3"))
    }

    println("Process finished with exit code " + ret)

    if (ret != 0) {
      println("Shell job returned error code, job aborted")
      System.exit(1)
    }

    showcounts(ht.toString)
  }

  def final_tbl(client: String, sdate: String) {
    val varsmap = getvars(client, sdate)
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

    val history_tbl = sc.textFile(varsmap("history_tbl")).toDF("id")
    //    val tagl = scala.io.Source.fromFile(varsmap("tag_file")).getLines.map(l => l.split(" +")).toList
    val tagl = sc.textFile(varsmap("tag_file")).map(l=>l.split(" +")).collect().toList
    //al.foreach(l => println(l.mkString(",")))

    val tbl = data.reduce(_ union _)
      .join(history_tbl, Seq("id"), "leftanti")
      .map({r =>
        val tag = tagl.filter({x =>
          val tags = x(0).replace(ltagdlm, " ").split(" +").toList
          tags.forall(r(1).toString.contains(_))
        }).map(x => x(1)).headOption.getOrElse("")
        (r(0).toString, r(1).toString, tag)
      })
      .filter(x => x._3 != "").dropDuplicates

    tbl.coalesce(10).write.format("com.databricks.spark.csv")
      .option("delimiter", varsmap("output_dlm")).save(varsmap("final_tbl"))

    showcounts(varsmap("final_tbl"))
  }

  def kv_tbl(client: String, sdate: String, tag: String, batch: Int) {
    val varsmap = getvars(client, sdate)
    val ldlm = varsmap("output_dlm")
    val ltagdlm = varsmap("tag_dlm")

    println("Running kv_tbl")

    val df = sc.textFile(varsmap("final_tbl")).toDF
      .withColumn("_tmp", split($"value", varsmap("output_dlm")))
      .select(
        $"_tmp".getItem(0).as("mobile"),
        $"_tmp".getItem(1).as("pattern"),
        $"_tmp".getItem(2).as("tag_prefix")
      ).drop($"_tmp").dropDuplicates().toDF()

    val df2 = if (tag == "all") df else df.filter("tag_prefix = '" + tag + "'")

    val counts = df2.groupBy("tag_prefix").count().collect()

    counts.zipWithIndex.foreach({r =>
      println("Tag " + r._1.get(0) + " has " + r._1.get(1) + " rows")

      val key = r._1.get(0).toString + "_" + "%02d".format(r._2 + batch) + "_" + today

      val df3 = df2.filter("tag_prefix = '" + r._1.get(0).toString + "'")

      val tbl = df3.rdd.zipWithIndex().map({x =>
        val pattern = x._1.get(1).toString.replace(ltagdlm, " ").split(" +").toArray
        (key + "_" + x._2, "ad" + ldlm + pattern(1) + ":" + key + ldlm + x._1.get(0).toString)
      }).union(sc.parallelize(Seq((key + "_total", r._1.get(1).toString))))
        .toDF()

      tbl.coalesce(10).write.format("com.databricks.spark.csv")
        .option("delimiter", varsmap("output_dlm")).save(varsmap("kv_tbl") + "_" + key)

      showcounts(varsmap("kv_tbl") + "_" + key)
    })
  }

  def kv_enc_tbl(client: String, sdate: String, pfx: String, source: String = "_s1") {
    val varsmap = getvars(client, sdate)
    val sources = getsources(client)

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

  def run_all(client: String, sdate: String, tag:String, batch: Int, pfx: String) {
    stg_s1(client, sdate)
    stg_s2(client, sdate)
    stg_s3(client, sdate, 0)
    stg_s3(client, sdate, 1)
    final_tbl(client, sdate)
    kv_tbl(client, sdate, tag, batch)
    kv_enc_tbl(client, sdate, pfx, "_s0")
  }

  def run_all_with_stg_s0(client: String, sdate: String, tag:String, batch: Int, pfx: String) {
    stg_s1(client, sdate, "_s0")
    stg_s2(client, sdate)
    stg_s3(client, sdate, 0)
    stg_s3(client, sdate, 1)
    final_tbl(client, sdate)
    kv_tbl(client, sdate, tag, batch)
    kv_enc_tbl(client, sdate, pfx)
  }

  def main(args: Array[String]) {
    args(0) match {
      case "stg_s0" => stg_s0(args(1))
      case "stg_s1" => stg_s1(args(1), args(2))
      case "stg_s1_with_stg_s0" => stg_s1(args(1), args(2), "_s0")
      case "stg_s2" => stg_s2(args(1), args(2))
      case "stg_s3_acc" => stg_s3(args(1), args(2), 0)
      case "stg_s3_fuz" => stg_s3(args(1), args(2), 1)
      case "final_tbl" => final_tbl(args(1), args(2))
      case "kv_tbl" => kv_tbl(args(1), args(2), args(3), args(4).toInt)
      case "kv_enc_tbl" => kv_enc_tbl(args(1), args(2), args(3))
      case "kv_enc_tbl_with_stg_s0" => kv_enc_tbl("all", args(1), args(2), "_s0")
      case "run_all" => run_all(args(1), args(2), args(3), args(4).toInt, args(5))
      case "run_all_with_stg_s0" => run_all_with_stg_s0(args(1), args(2), args(3), args(4).toInt, args(5))
    }
  }
}

