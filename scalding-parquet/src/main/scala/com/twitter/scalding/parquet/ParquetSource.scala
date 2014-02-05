package com.twitter.scalding.parquet

import cascading.tuple.Fields
import com.twitter.scalding._
import _root_.parquet.cascading.{ParquetTBaseScheme, ParquetTupleScheme}
import _root_.parquet.org.apache.thrift.{TFieldIdEnum, TBase}
import cascading.scheme.Scheme
import com.twitter.scalding.typed.{TypedSink, TypedSource}
import com.etsy.cascading.tap.local.LocalTap

object ParquetSource {
  def apply(fields: Fields, path: String) =
    new ParquetSource(fields, Seq(path))

  def apply(fields: Fields, paths: Seq[String]) =
    new ParquetSource(fields, paths)
}

class ParquetSource(fields: Fields, paths: Seq[String]) extends FixedPathSource(paths: _*) {
  override def hdfsScheme = HadoopSchemeInstance(new ParquetTupleScheme(fields).asInstanceOf[Scheme[_,_,_,_,_]])
}

trait ParquetTBase[T]
  extends Mappable[T]
  with TypedSource[T]
  with TypedSink[T]
  /* with LocalTapSource get the etsy tap for local mode */ {

  def manifest: Manifest[T] // subclasses do the implicit thing
  def tset: TupleSetter[T]

  val klass = manifest.erasure.asSubclass[TBase[_, _ <: TFieldIdEnum]](classOf[TBase[_, _ <: TFieldIdEnum]])

  def hdfsScheme = HadoopSchemeInstance(new ParquetTBaseScheme(klass).asInstanceOf[Scheme[_,_,_,_,_]])

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](implicitly[TupleConverter[T]])
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](tset)
}

case class FixedPathParquetTBaseSource[T](path: String)
                                         (implicit override val manifest: Manifest[T], val tset: TupleSetter[T])
  extends FixedPathSource(path)
  with ParquetTBase[T]
