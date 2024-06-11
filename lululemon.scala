import java.io.File
import java.io.FileWriter
import sttp.client4._
import sttp.client4.httpclient.HttpClientSyncBackend
import zio._
import zio.json._

val cik = Map("lululemon" -> "0001397187")

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
  id VARCHAR(255) PRIMARY KEY,
  label VARCHAR(255),
  description TEXT
);

CREATE TABLE lululemon.financials (
  document VARCHAR(20) NOT NULL,
  tag VARCHAR(255) NOT NULL,
  value DECIMAL(20, 4) NOT NULL,
  unit VARCHAR(8) NOT NULL,
  PRIMARY KEY (document, tag),
  FOREIGN KEY (tag) REFERENCES xbrl(id)
);

CREATE USER 'device'@'%' IDENTIFIED BY 'CL0UD5Q1';
GRANT ALL PRIVILEGES ON lululemon.* TO 'device'@'%';
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

object SECValue {
  implicit val decoder: JsonDecoder[SECValue] = DeriveJsonDecoder.gen[SECValue]
}

object SECXBRL {
  implicit val decoder: JsonDecoder[SECXBRL] = DeriveJsonDecoder.gen[SECXBRL]
}

object SECFile {
  implicit val decoder: JsonDecoder[SECFile] = DeriveJsonDecoder.gen[SECFile]
}

def fetch(cik: String): Option[SECFile] = {
  implicit val backend = HttpClientSyncBackend()

  val request = basicRequest
    .get(uri"https://data.sec.gov/api/xbrl/companyfacts/CIK$cik.json")
    .header("User-Agent", "Scala/1.0")

  val response = request.send(backend)

  response.body.toOption match {
    case Some(json) => json.fromJson[SECFile].toOption
    case _ => None
  }
}

def ixbrl(tag: String): String = {
  tag.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase
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

def sql(file: SECFile): Unit = {
  val cik10 = f"${file.cik}%010d"
  val fileName = s"$cik10.sql"
  val statement = s"INSERT INTO sec (cik, registrant) VALUES\n('$cik10', '${file.entityName}');\n"

  sqlwrite(fileName, statement)

  val xbrl = for {
    (namespace, xbrls) <- file.facts.toList
    (tag, xbrl) <- xbrls.toList
  } yield s"(\"${ixbrl(tag)}\", ${sqlcast(xbrl.label)}, ${sqlcast(xbrl.description)})"

  if (xbrl.nonEmpty) {
    val statement = s"INSERT IGNORE INTO xbrl (id, label, description) VALUES\n${xbrl.mkString(",\n")};\n"

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
  fetch(cik("lululemon")).foreach(sql)
}
