val t = spark.createDataFrame(Seq(
    ("B8", 0.04, 0.34, 0.62, 0.01, 0.10, 0.11, 1.00),
    ("A0", 0.72, 1.04, 1.28, 0.54, 0.58, 0.56, 0.30),
    ("A2", 1.39, 1.65, 1.87, 1.12, 1.15, 1.12, 1.10),
    ("A5", 1.95, 2.15, 2.32, 1.53, 1.52, 1.48, 1.75),
    ("A7", 2.23, 2.40, 2.55, 1.75, 1.71, 1.66, 2.08),
    ("F0", 2.68, 2.79, 2.90, 2.10, 2.01, 1.96, 2.61),
    ("F2", 2.96, 3.04, 3.13, 2.32, 2.20, 2.14, 2.89),
    ("F5", 3.68, 3.69, 3.74, 2.85, 2.67, 2.61, 3.61),
    ("F8", 4.29, 4.26, 4.28, 3.31, 3.08, 3.01, 4.24),
    ("G0", 4.52, 4.44, 4.44, 3.53, 3.27, 3.20, 4.47),
    ("G2", 4.65, 4.54, 4.51, 3.64, 3.38, 3.30, 4.60),
    ("G5", 4.92, 4.79, 4.74, 3.86, 3.56, 3.48, 4.89),
    ("G8", 5.50, 5.32, 5.25, 4.31, 3.95, 3.86, 5.30),
    ("K0", 5.77, 5.55, 5.45, 4.49, 4.10, 4.00, 5.69),
    ("K2", 6.23, 5.94, 5.80, 4.80, 4.35, 4.24, 6.08),
    ("K4", 6.77, 6.40, 6.20, 5.08, 4.56, 4.43, 6.55),
    ("K5", 7.03, 6.59, 6.35, 5.20, 4.64, 4.51, 6.68),
    ("K7", 7.45, 6.90, 6.58, 5.46, 4.85, 4.70, 6.89),
    ("M0", 8.50, 7.83, 7.46, 6.04, 5.37, 5.18, 7.60),
    ("M1", 9.00, 8.12, 7.64, 6.33, 5.68, 5.47, 7.97),
    ("M2", 9.76, 8.73, 8.15, 6.73, 6.09, 5.86, 8.44),
    ("M3", 10.77, 9.44, 8.74, 7.31, 6.68, 6.44, 9.09),
    ("M4", 11.99, 10.48, 9.64, 8.10, 7.49, 7.22, 9.92),
    ("M5", 13.67, 11.76, 10.71, 9.08, 8.47, 8.16, 11.01),
    ("M6", 14.99, 12.98, 11.88, 10.15, 9.50, 9.16, 12.06),
    ("M7", 16.21, 13.94, 12.68, 10.76, 10.08, 9.69, 12.70),
    ("M8", 17.60, 14.83, 13.21, 11.19, 10.46, 10.03, 13.13),
    ("M9", 18.19, 15.38, 13.69, 11.49, 10.73, 10.26, 13.43),
    ("L0", 18.48, 15.85, 14.01, 11.76, 10.96, 10.44, 13.69)
    )).toDF("SpT", "MI", "MJ", "MK", "MW1", "MW2", "MW3", "MW4")
t.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_spt");

val k = spark.createDataFrame(Seq(
    lit(1.71).as("I"),
    lit(0.72).as("J"),
    lit(0.30).as("K"),
    lit(0.18).as("W1"),
    lit(0.16).as("W2"),
    lit(0.14).as("W2"),
    lit(0.11).as("W2")
)).toDF("lambda", "koef")
k.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_koef");

val beta = spark.range(100).select(((col("id")+1)*30).as("beta"))
val b = spark.range(100).select(((col("id")+1)*30).as("b"))
val a_0 = spark.range(100).select(((col("id")+1)/100).as("a_0"))
beta.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_beta");
b.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_b");
a_0.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_a_0");

val t =spark.read.table("user_dcherkashin.vkr_spt")
val beta = spark.read.table("user_dcherkashin.vkr_beta")
val b = spark.read.table("user_dcherkashin.vkr_b")
val a_0 = spark.read.table("user_dcherkashin.vkr_a_0")

val params = t
.crossJoin(b)
.crossJoin(a_0)
.crossJoin(beta)
params.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_params")

val cj = spark.read.table("user_dcherkashin.vkr_2_cross_match")
val params = spark.read.table("user_dcherkashin.vkr_params")
val rrr = cj.select(
  col("RAJ20002").cast(DoubleType).as("RAJ2000"),
  col("DEJ20003").cast(DoubleType).as("DEJ2000"),
  col("DENIS").as("DENIS"),
  col("AllWISE").as("AllWISE"),
  col("Imag").cast(DoubleType).as("Imag"),
  lit(1.71).cast(DoubleType).as("I"),
  col("e_Imag").cast(DoubleType).as("e_Imag"),
  coalesce(col("Jmag5"),col("Jmag23")).cast(DoubleType).as("Jmag"),
  lit(0.72).cast(DoubleType).as("J"),
  coalesce(col("e_Jmag8"),col("e_Jmag30")).cast(DoubleType).as("e_Jmag"),
  coalesce(col("Kmag6"),col("Kmag25")).cast(DoubleType).as("Kmag"),
  lit(0.30).cast(DoubleType).as("K"),
  coalesce(col("e_Kmag9"),col("e_Kmag32")).cast(DoubleType).as("e_Kmag"),
  col("W1mag").cast(DoubleType).as("W1mag"),
  lit(0.18).cast(DoubleType).as("W1"),
  col("e_W1mag").cast(DoubleType).as("e_W1mag"),
  col("W2mag").cast(DoubleType).as("W2mag"),
  lit(0.16).cast(DoubleType).as("W2"),
  col("e_W2mag").cast(DoubleType).as("e_W2mag"),
  col("W3mag").cast(DoubleType).as("W3mag"),
  lit(0.14).cast(DoubleType).as("W3"),
  col("e_W3mag").cast(DoubleType).as("e_W3mag"),
  col("W4mag").cast(DoubleType).as("W4mag"),
  lit(0.11).cast(DoubleType).as("W4"),
  col("e_W4mag").cast(DoubleType).as("e_W4mag")
).where(
  col("RAJ2000").isNotNull and
  col("DEJ2000").isNotNull and
  col("DENIS").isNotNull and
  col("AllWISE").isNotNull and
  col("Imag").isNotNull and
  col("e_Imag").isNotNull and
  col("Jmag").isNotNull and
  col("e_Jmag").isNotNull and
  col("Kmag").isNotNull and
  col("e_Kmag").isNotNull and
  col("W1mag").isNotNull and
  col("e_W1mag").isNotNull and
  col("W2mag").isNotNull and
  col("e_W2mag").isNotNull and
  col("W3mag").isNotNull and
  col("e_W3mag").isNotNull and
  col("W4mag").isNotNull and
  col("e_W4mag").isNotNull
)
.limit(100)
.crossJoin(params)
.repartition(200)
rrr.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_cross_validation");

val ttttt = spark.read.table("user_dcherkashin.vkr_cross_validation")

val asd = ttttt
.select(
  col("RAJ2000"),
  col("DEJ2000"),
  col("DENIS"),
  col("AllWISE"),
  col("I"),
  col("MI"),
  col("Imag"),
  col("e_Imag"),
  col("J"),
  col("MJ"),
  col("Jmag"),
  col("e_Jmag"),
  col("K"),
  col("MK"),
  col("Kmag"),
  col("e_Kmag"),
  col("W1"),
  col("MW1"),
  col("W1mag"),
  col("e_W1mag"),
  col("W2"),
  col("MW2"),
  col("W2mag"),
  col("e_W2mag"),
  col("W3"),
  col("MW3"),
  col("W3mag"),
  col("e_W3mag"),
  col("W4"),
  col("MW4"),
  col("W4mag"),
  col("e_W4mag"),
  col("SpT"),
  col("b"),
  col("a_0"),
  col("beta"),
  (
    pow(((col("Imag")  - (col("MI")  + lit(5) * log(col("b")) - lit(5) + col("I") * (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_Imag"))), lit(2))  +
    pow(((col("Jmag")  - (col("MJ")  + lit(5) * log(col("b")) - lit(5) + col("J") * (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_Jmag"))), lit(2))  +
    pow(((col("Kmag")  - (col("MK")  + lit(5) * log(col("b")) - lit(5) + col("K") * (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_Kmag"))), lit(2))  +
    pow(((col("W1mag") - (col("MW1") + lit(5) * log(col("b")) - lit(5) + col("W1")* (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_W1mag"))), lit(2)) +
    pow(((col("W2mag") - (col("MW2") + lit(5) * log(col("b")) - lit(5) + col("W2")* (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_W2mag"))), lit(2)) +
    pow(((col("W3mag") - (col("MW3") + lit(5) * log(col("b")) - lit(5) + col("W3")* (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_W3mag"))), lit(2)) +
    pow(((col("W4mag") - (col("MW4") + lit(5) * log(col("b")) - lit(5) + col("W4")* (col("a_0") * col("beta") * (lit(1) - exp((- col("b") * sin(abs(col("DEJ2000"))))))/sin(abs(col("DEJ2000"))))))/(col("e_W4mag"))), lit(2))
  ).as("functional")
)
asd.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_functional");


val res_0 = spark.read.table("user_dcherkashin.vkr_functional")
val res = res_0
.groupBy(col("DENIS"), col("AllWISE"))
.agg(min(col("functional")).as("functional"))
.join(res_0, Seq("DENIS", "AllWISE", "functional"))

res.write.mode("overwrite").saveAsTable("user_dcherkashin.vkr_res");
