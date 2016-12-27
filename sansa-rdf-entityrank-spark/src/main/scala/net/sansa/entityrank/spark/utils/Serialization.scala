package net.sansa.entityrank.spark.utils

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import com.esotericsoftware.kryo.Kryo
import net.sansa.entityrank.spark.model.EntityRank
import net.sansa.entityrank.spark.model.Triples
/*
 * Class for serialization by the Kryo serializer.
 */
class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[EntityRank])
    kryo.register(classOf[Triples])
  }
}