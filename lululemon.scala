import java.io.File
import java.io.FileWriter

val schemaSQL = """
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
  quarter ENUM('Q1', 'Q2', 'Q3', 'Q4', 'FY') NOT NULL,
  start_date DATE NOT NULL,
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
  value BIGINT NOT NULL,
  unit VARCHAR(8) NOT NULL,
  PRIMARY KEY (document, tag),
  FOREIGN KEY (tag) REFERENCES xbrl(id)
);

CREATE USER 'device'@'%' IDENTIFIED BY 'CL0UD5Q1';
GRANT ALL PRIVILEGES ON lululemon.* TO 'device'@'%';
FLUSH PRIVILEGES;
""".stripMargin.trim

def sqlout(path: String, statement: String): Unit = {
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
  sqlout("schema.sql", schemaSQL)
}
