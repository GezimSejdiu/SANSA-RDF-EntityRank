package net.sansa_stack.entityrank.flink.utils
import org.slf4j.LoggerFactory

object FlinkSettings {

  /**
   * Set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) =
    {
      loggers.map {
        loggerName =>
          val logger = org.apache.log4j.Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel()
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }

      //setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "flink", "org.eclipse.jetty", "akka"))
}