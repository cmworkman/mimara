package models

import java.io.File

import play.api.libs.json._
import play.api.libs.functional.syntax._
import milliman.mara.engine.ModelEngineBase
import milliman.mara.engine.ModelEngineFactory
import milliman.mara.exception._
import milliman.mara.model._
import milliman.mara.model.Enums.{DependentStatus, Gender, ModelName}
import milliman.mara.batchProcessor.BatchProperties
import milliman.mara.model.hhs.{HhsInputEnums, HhsOutputModelScore}
import java.util
import java.sql.Date
import java.text.DateFormat
import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters._


case class SubmitRequest(
  modelType: String,
)

class ProcessMara {

  private val MARA_FILES_PATH = s"""${System.getProperty("user.dir")}${File.separator}MaraFiles"""
  private var engineFactory: Option[ModelEngineFactory] = None
  private var modelProcessor: Option[ModelEngineBase] = None

  private def getModelToRun(model: String) = {
    model match {
      case "COMXPLN" => "CxXPLNCon,CxXPLNPro,DxXPLNCon,DxXPLNPro,RxXPLNCon,RxXPLNPro,AsXPLNCon,AsXPLNPro"
      case "COMOPTML" => "CxOPTMLCon,CxOPTMLPro,DxOPTMLCon,DxOPTMLPro,RxOPTMLCon,RxOPTMLPrO"
      case "MDCRXPLN" => "MCRCxXPLNCon,MCRDxXPLNCon,MCRRxXPLNCon,MCRCxXPLNPro,MCRDxXPLNPro,MCRRxXPLNPro,MCRAsXPLNPro,MCRAsXPLNCon"
      case "MDCROPTML" => "MCRCxOPTmlCon,MCRDxOPTmlCon,MCRRxOPTmlCon,MCRCxOPTmlPro,MCRDxOPTmlPro,MCRRxOPTmlPro"
      case "RR" => "RisingRisk"
    }
  }

  private def init(model: String) = {
    val bp = new BatchProperties()
    bp.setDateFormat("MM/dd/yyyy")
    bp.setLicenseFileLocation(s"${MARA_FILES_PATH}/mara.lic")
    bp.setMaraAppFolderLocation(s"${MARA_FILES_PATH}/MaraData/")
    bp.setApplyPriorCostProspective(false)
    bp.setOutputPercentContributions(false)
    bp.setModelList(model)
    bp.setModelOutputScale(3)

    engineFactory = Some(new ModelEngineFactory(new ModelProperties(bp)))
    modelProcessor = Some(engineFactory.get.getModelEngine())

    Some(true)
  }


  def submit(maraRequest: SubmitRequest) = {

    val modelToRun = getModelToRun(maraRequest.modelType)
    val initResult = init(modelToRun)
    process()

    8
  }

  private def process() = {
    val inputMedClaimList = new util.ArrayList[InputMedClaim]
    val inputMedClaim = new InputMedClaim

    val df = new SimpleDateFormat("MM/dd/yyyy")

    val inputMember = new InputMember();
    inputMember.setMemberId("ABC001");
    inputMember.setDob(df.parse("07/29/1947"));
    inputMember.setGender(Gender.FEMALE); //Needed (,RR)

    inputMedClaim.setFromDate(df.parse("11/1/2017"));
    inputMedClaim.setToDate(df.parse("11/1/2017")); //Needeed for HHS
    inputMedClaim.setPaidDate(df.parse("11/1/2017"));

    inputMedClaim.setRevCode("0120");
    inputMedClaim.setProviderId("92813");
    inputMedClaim.setSpecialty("XX111222334");
    inputMedClaim.setPos("8"); //Needed for Commercial
    inputMedClaim.setCharged(111231.45);
    inputMedClaim.setAllowed(111231.45);
    inputMedClaim.setPaid(123.45);

    val diagList = new util.ArrayList[String]();
    diagList.add("2501");

    inputMedClaim.setDiagList(diagList);
    inputMedClaimList.add(inputMedClaim);

    val inputRxClaimList = new util.ArrayList[InputRxClaim]();
    val inputRxClaim = new InputRxClaim();
    inputRxClaim.setMemberId("ABC001");
    inputRxClaim.setNdcCode("00011122233");

    inputRxClaim.setClaimId("001");
    inputRxClaim.setFillDate(df.parse("12/1/2017"));
    inputRxClaim.setCharged(12.34);
    inputRxClaim.setAllowed(12.34);
    inputRxClaim.setPaid(12239.34);
    inputRxClaim.setDaysSupplied(30);
    inputRxClaim.setQtyDispensed(100);
    inputRxClaimList.add(inputRxClaim);


    inputMember.setDependentStatus(DependentStatus.CHILD);
    inputMember.setExposureMonths(12);
    inputMember.setInputMedClaim(inputMedClaimList);
    inputMember.setInputRxClaim(new util.ArrayList[InputRxClaim]());

    val date ="2019-05-31";
    val endBasisDate = Date.valueOf(date);//converting string into sql date

    val outputResultSet = modelProcessor.get.processMember(inputMember, endBasisDate);

    val modelDataMap = outputResultSet.getOutputModelDataMap

    modelDataMap.entrySet().forEach(entry => {
      val outputModelScore = entry.getValue.getOutputModelScore
      val modelName = entry.getKey

      val totScore = outputModelScore.getTotScore
      System.out.println(modelName + " Total: " + totScore)
    })
  }

}