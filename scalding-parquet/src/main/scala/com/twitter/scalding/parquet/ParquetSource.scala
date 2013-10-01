package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding.{HadoopSchemeInstance, FixedPathSource}
import parquet.cascading.ParquetTupleScheme

object ParquetSource {
  def apply(fields: Fields, path: String) =
    new ParquetSource(fields, Seq(path))

  def apply(fields: Fields, paths: Seq[String]) =
    new ParquetSource(fields, paths)
}

class ParquetSource(fields: Fields, paths: Seq[String]) extends FixedPathSource(paths: _*) {
  override def hdfsScheme = HadoopSchemeInstance(new ParquetTupleScheme(fields))
}
