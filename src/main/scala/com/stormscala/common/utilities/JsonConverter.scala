package com.stormscala.common.utilities

import org.apache.commons.text.StringEscapeUtils.escapeJson

import scala.collection.mutable.ListBuffer

// Adapted from: https://stackoverflow.com/questions/6271386/how-do-you-serialize-a-map-to-json-in-scala
object JsonConverter {
  def toJson(o: Any) : String = {
    var json = new ListBuffer[String]()
    o match {
      case m: Map[_,_] =>
        for ( (k,v) <- m ) {
          val key = escapeJson(k.asInstanceOf[String])
          v match {
            case a: Map[_,_] => json += "\"" + key + "\": " + toJson(a)
            case a: List[_] => json += "\"" + key + "\": " + toJson(a)
            case a: Array[_] => json += "\"" + key + "\": " + toJson(a)
            case a: java.lang.Number => json += "\"" + key + "\": " + a.toString
            case a: Boolean => json += "\"" + key + "\": " + "\"" + a.toString + "\""
            case a: String =>
              if(a.equals("null")) { json += "\"" + key + "\": " + a }
              else { json += "\"" + key + "\":\"" + escapeJson(a) + "\"" }
            case _ => ;
          }
        }
      case m: Array[_] =>
        var list = new ListBuffer[String]()
        for ( el <- m ) {
          el match {
            case a: Map[_,_] => list += toJson(a)
            case a: List[_] => list += toJson(a)
            case a: Array[_] => list += toJson(a)
            case a: java.lang.Number => list += "\"" + a.toString + "\""
            case a: Boolean => list += "\"" + a.toString + "\""
            case a: String =>
              if(a.equals("null")) { list += a }
              else { list += "\"" + escapeJson(a) + "\"" }
            case _ => ;
          }
        }
        return "[" + list.mkString(",") + "]"
      case m: List[_] =>
        var list = new ListBuffer[String]()
        for ( el <- m ) {
          el match {
            case a: Map[_,_] => list += toJson(a)
            case a: List[_] => list += toJson(a)
            case a: Array[_] => list += toJson(a)
            case a: java.lang.Number => list += "\"" + a.toString + "\""
            case a: Boolean => list += "\"" + a.toString + "\""
            case a: String =>
              if(a.equals("null")) { list += a }
              else { list += "\"" + escapeJson(a) + "\"" }
            case _ => ;
          }
        }
        return "[" + list.mkString(",") + "]"
      case _ => ;
    }
    "{" + json.mkString(",") + "}"
  }
}
