import java.io.File
import java.io.FileWriter
import java.time.LocalDate
import java.time.ZoneId
import scala.util.Try
import sttp.client4._
import sttp.client4.httpclient.HttpClientSyncBackend
import zio._
import zio.json._

enum SEC(val cik: String):
  case lululemon extends SEC("0001397187")

enum Stock(val exchange: String, val ticker: String, val sec: SEC):
  case lululemon extends Stock("NASDAQ", "LULU", SEC.lululemon)

val schema = """
DROP USER IF EXISTS device;
DROP DATABASE IF EXISTS lululemon;
CREATE DATABASE lululemon;

CREATE TABLE lululemon.sec (
  cik VARCHAR(10) PRIMARY KEY,
  registrant VARCHAR(255) NOT NULL
);

CREATE TABLE lululemon.documents (
  accn VARCHAR(20) PRIMARY KEY,
  form ENUM('10-Q', '10-K') NOT NULL,
  year SMALLINT UNSIGNED NOT NULL,
  period ENUM('Q1', 'Q2', 'Q3', 'Q4', 'FY') NOT NULL,
  end_date DATE NOT NULL,
  file_date DATE NOT NULL
);

CREATE TABLE lululemon.xbrl (
  tag VARCHAR(255) PRIMARY KEY,
  label VARCHAR(255),
  description TEXT
);

CREATE TABLE lululemon.financials (
  document VARCHAR(20) NOT NULL,
  tag VARCHAR(255) NOT NULL,
  value DECIMAL(20, 4) NOT NULL,
  unit VARCHAR(12) NOT NULL,
  PRIMARY KEY (document, tag),
  FOREIGN KEY (tag) REFERENCES xbrl(tag)
);

CREATE TABLE lululemon.stock_exchange (
  stock VARCHAR(5) PRIMARY KEY,
  exchange VARCHAR(8) NOT NULL,
  cik VARCHAR(10) NOT NULL,
  FOREIGN KEY (cik) REFERENCES sec(cik)
);

CREATE TABLE lululemon.quotes (
  stock VARCHAR(5) NOT NULL,
  date DATE NOT NULL,
  open DECIMAL(12, 4) NOT NULL,
  high DECIMAL(12, 4) NOT NULL,
  low DECIMAL(12, 4) NOT NULL,
  close DECIMAL(12, 4) NOT NULL,
  adj_close DECIMAL(12, 4) NOT NULL,
  volume BIGINT NOT NULL,
  PRIMARY KEY (stock, date),
  FOREIGN KEY (stock) REFERENCES stock_exchange(stock)
);

CREATE USER 'device'@'%' IDENTIFIED WITH caching_sha2_password BY 'CL0UD5Q1';
GRANT SELECT ON lululemon.* TO 'device'@'%';
FLUSH PRIVILEGES;
""".stripMargin.trim

case class SECValue(
  end: String,
  `val`: BigDecimal,
  accn: String,
  fy: Int,
  fp: String,
  form: String,
  filed: String,
  frame: Option[String]
)

case class SECXBRL(
  label: Option[String],
  description: Option[String],
  units: Map[String, List[SECValue]]
)

case class SECFile(
  cik: Int,
  entityName: String,
  facts: Map[String, Map[String, SECXBRL]],
)

case class Quote(
  stock: Stock,
  date: String,
  open: BigDecimal,
  high: BigDecimal,
  low: BigDecimal,
  close: BigDecimal,
  adj_close: BigDecimal,
  volume: Long
)

object SECValue {
  implicit val decoder: JsonDecoder[SECValue] = DeriveJsonDecoder.gen[SECValue]
}

object SECXBRL {
  implicit val decoder: JsonDecoder[SECXBRL] = DeriveJsonDecoder.gen[SECXBRL]
}

object SECFile {
  implicit val decoder: JsonDecoder[SECFile] = DeriveJsonDecoder.gen[SECFile]
}

def ixbrl(tag: String): String = {
  tag.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase
}

def fetch(sec: SEC): Option[SECFile] = {
  implicit val backend = HttpClientSyncBackend()

  val request = basicRequest
    .get(uri"https://data.sec.gov/api/xbrl/companyfacts/CIK${sec.cik}.json")
    .header("User-Agent", "Scala/1.0")

  val response = request.send(backend)

  response.body.toOption match {
    case Some(json) => json.fromJson[SECFile].toOption
    case _ => None
  }
}

def fetch(stock: Stock): Option[List[Quote]] = {
  implicit val backend = HttpClientSyncBackend()

  val period1 = LocalDate.of(2000, 1, 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond
  val period2 = LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toEpochSecond

  val request = basicRequest
    .get(uri"https://query1.finance.yahoo.com/v7/finance/download/${stock.ticker}?period1=$period1&period2=$period2&interval=1d&events=history&includeAdjustedClose=true")
    .header("User-Agent", "Scala/1.0")

  val response = request.send(backend)

  response.body.toOption match {
    case Some(csv) => {
      val lines = csv.split("\n").toList

      val quotes = for {
        line <- lines.drop(1)
        fields = line.split(",")
        if fields.length == 7
      } yield Quote(
          stock = stock,
          date = fields(0),
          open = Try(BigDecimal(fields(1))).getOrElse(BigDecimal(0)),
          high = Try(BigDecimal(fields(2))).getOrElse(BigDecimal(0)),
          low = Try(BigDecimal(fields(3))).getOrElse(BigDecimal(0)),
          close = Try(BigDecimal(fields(4))).getOrElse(BigDecimal(0)),
          adj_close = Try(BigDecimal(fields(5))).getOrElse(BigDecimal(0)),
          volume = Try(fields(6).toLong).getOrElse(0L)
        )

      Some(quotes)
    }
    case _ => None
  }
}

def sql(file: SECFile): Unit = {
  val cik10 = f"${file.cik}%010d"
  val fileName = s"$cik10.sql"
  val statement = s"INSERT INTO sec (cik, registrant) VALUES\n(\"$cik10\", \"${file.entityName}\");\n"

  sqlwrite(fileName, statement)

  val xbrl = for {
    (namespace, xbrls) <- file.facts.toList
    (tag, xbrl) <- xbrls.toList
  } yield s"(\"${ixbrl(tag)}\", ${sqlcast(xbrl.label)}, ${sqlcast(xbrl.description)})"

  if (xbrl.nonEmpty) {
    val statement = s"INSERT IGNORE INTO xbrl (tag, label, description) VALUES\n${xbrl.mkString(",\n")};\n"

    sqlwrite(fileName, statement)
  }

  val documents = for {
    (namespace, xbrls) <- file.facts.toList
    (tag, xbrl) <- xbrls.toList
    unit <- xbrl.units.keys
    value <- xbrl.units(unit)
    if value.frame.nonEmpty
  } yield s"(\"${value.accn}\", \"${value.form}\", ${value.fy}, \"${value.fp}\", \"${value.end}\", \"${value.filed}\")"

  if (documents.nonEmpty) {
    val statement = s"INSERT IGNORE INTO documents (accn, form, year, period, end_date, file_date) VALUES\n${documents.mkString(",\n")};\n"

    sqlwrite(fileName, statement)
  }

  val financials = for {
    (namespace, xbrls) <- file.facts.toList
    (tag, xbrl) <- xbrls.toList
    unit <- xbrl.units.keys
    value <- xbrl.units(unit)
    if value.frame.nonEmpty
  } yield s"(\"${value.accn}\", \"${ixbrl(tag)}\", ${value.`val`}, \"${unit}\")"

  if (financials.nonEmpty) {
    val statement = s"INSERT IGNORE INTO financials (document, tag, value, unit) VALUES\n${financials.mkString(",\n")};\n"

    sqlwrite(fileName, statement)
  }
}

def sql(quotes: List[Quote]): Unit = {
  if (quotes.isEmpty) return

  val stock = quotes.head.stock
  val fileName = s"${stock.sec.cik}.sql"
  val statement = s"INSERT INTO stock_exchange (stock, exchange, cik) VALUES\n (\"${stock.ticker}\", \"${stock.exchange}\", \"${stock.sec.cik}\");\n"

  sqlwrite(fileName, statement)

  val data = quotes.map { quote =>
    s"(\"${quote.stock.ticker}\", \"${quote.date}\", ${quote.open}, ${quote.high}, ${quote.low}, ${quote.close}, ${quote.adj_close}, ${quote.volume})"
  }

  val statement2 = s"INSERT IGNORE INTO quotes (stock, date, open, high, low, close, adj_close, volume) VALUES\n${data.mkString(",\n")};\n"

  sqlwrite(fileName, statement2)
}

def sqlcast(option: Option[Any]): String = {
  option match {
    case Some(value) => {
      value match {
        case string: String => s"\"$string\""
        case _ => "NULL"
      }
    }
    case _ => "NULL"
  }
}

def sqlwrite(path: String, statement: String): Unit = {
  val file = new File(s"sql/$path")
  val directory = file.getParentFile

  if (!directory.exists) directory.mkdirs()
  if (!file.exists) file.createNewFile()
  if (statement.isEmpty) return

  val writer = new FileWriter(file, true)

  try {
    writer.write(s"$statement\n")
  } finally {
    writer.close()
  }
}

@main def script(args: String*): Unit = {
  sqlwrite("schema.sql", schema)
  fetch(SEC.lululemon).foreach(sql)
  fetch(Stock.lululemon).foreach(sql)
}
