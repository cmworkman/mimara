// Databricks notebook source
// I previously created a Databricks mount
/**
  val AwsBucketName = "midatabricksbucket"
  val MountName = "mara"

  dbutils.fs.unmount(s"/mnt/$MountName")
  dbutils.fs.mount(s"s3a://$AwsBucketName/mara", s"/mnt/$MountName")
  display(dbutils.fs.ls(s"/mnt/$MountName"))
**/


// Migrate from mount created above to local FUSE location
dbutils.fs.cp("/mnt/mara/MaraFiles/MaraFilesHRT/License/mara.lic", "dbfs:/Mara433/mara.lic")
dbutils.fs.cp("/mnt/mara/MaraFiles/MaraFilesHRT/Data/", "dbfs:/Mara433/MaraData/", true)

// COMMAND ----------

import milliman.mara.engine.ModelEngineBase
import milliman.mara.engine.ModelEngineFactory
import milliman.mara.exception._
import milliman.mara.model._
import milliman.mara.model.Enums.{DependentStatus, Gender, ModelName}
import org.apache.spark.sql.{Dataset, SparkSession}
import milliman.mara.batchProcessor.BatchProperties
import milliman.mara.model.hhs.{HhsInputEnums, HhsOutputModelScore}
import java.util

val modelsToRun = "COMXPLN"

val models = modelsToRun match {
 case "COMXPLN" => "CxXPLNCon,CxXPLNPro,DxXPLNCon,DxXPLNPro,RxXPLNCon,RxXPLNPro,AsXPLNCon,AsXPLNPro"
 case "COMOPTML" => "CxOPTMLCon,CxOPTMLPro,DxOPTMLCon,DxOPTMLPro,RxOPTMLCon,RxOPTMLPrO"
 case "MDCRXPLN" => "MCRCxXPLNCon,MCRDxXPLNCon,MCRRxXPLNCon,MCRCxXPLNPro,MCRDxXPLNPro,MCRRxXPLNPro,MCRAsXPLNPro,MCRAsXPLNCon"
 case "MDCROPTML" => "MCRCxOPTmlCon,MCRDxOPTmlCon,MCRRxOPTmlCon,MCRCxOPTmlPro,MCRDxOPTmlPro,MCRRxOPTmlPro"
 case "RR" => "RisingRisk"
}

val bp = new BatchProperties()
bp.setDateFormat("MM/dd/yyyy")
bp.setLicenseFileLocation("/dbfs/Mara433/mara.lic")
bp.setMaraAppFolderLocation("/dbfs/Mara433/MaraData/")
bp.setApplyPriorCostProspective(false)
bp.setOutputPercentContributions(false)
bp.setModelList(models)
bp.setModelOutputScale(3)

val engineFactory = new ModelEngineFactory(new ModelProperties(bp))
val modelProcessor = Some(engineFactory.getModelEngine())

// COMMAND ----------

import scala.collection.JavaConverters._
import java.sql.Date

val inputMedClaimList = new util.ArrayList[InputMedClaim]
val inputMedClaim = new InputMedClaim

import java.text.DateFormat
import java.text.SimpleDateFormat
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

import milliman.mara.model.OutputModelData
import milliman.mara.model.OutputModelScore
val modelDataMap = outputResultSet.getOutputModelDataMap


import scala.collection.JavaConversions._
for (entry <- modelDataMap.entrySet) {
  val outputModelScore = entry.getValue.getOutputModelScore
  val modelName = entry.getKey

  val totScore = outputModelScore.getTotScore
  System.out.println(modelName + " Total: " + totScore)

  val percentContMap = entry.getValue.getOutputPercentContribution match {
        case null => None
        case x => Some(x)
      }

  outputResultSet.getConditionList.map(currentCategory => {

    println(currentCategory.getVariable)
    val modelPercent = percentContMap match {
      case None => 0.0
      case _ => if (percentContMap.get.entrySet().
        filter(c => c.getKey == currentCategory.getVariable).head != null) {

        val percCont = percentContMap.get.entrySet().filter(c => c.getKey == currentCategory.getVariable).head.getValue.toDouble
        println(currentCategory)
        percCont

      } else { 0.0 }
    }

    println(modelPercent)
  })
}