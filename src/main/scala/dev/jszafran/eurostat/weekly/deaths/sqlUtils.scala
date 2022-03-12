package dev.jszafran.eurostat.weekly.deaths

object sqlUtils {
  def generateStackExpr(alias: String, colNames: String*): String = {
    val exprBegin      = s"stack(${colNames.size},"
    val colsDefinition = colNames.map(c => s"'$c', `$c`").mkString(", ") + ")"
    s"$exprBegin $colsDefinition as $alias"
  }
}
