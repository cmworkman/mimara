package models

import play.api.libs.json._
import play.api.libs.functional.syntax._

case class Person(
  name: String,
  country: String,
  id: Int
)

//implicit val residentReads = Json.reads[Resident]