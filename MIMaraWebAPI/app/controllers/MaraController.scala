package controllers

import javax.inject._
import models.Person
import play.api._
import play.api.libs.json.{JsValue, Json}
import play.api.mvc._

@Singleton
class MaraController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def submit = Action { implicit request =>

    val json = request.body.asJson.get
    implicit val residentReads = Json.reads[Person]
    val person = json.as[Person]
    Ok("name : " + person.name)
  }
}
