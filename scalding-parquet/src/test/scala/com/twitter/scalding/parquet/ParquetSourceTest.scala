package com.twitter.scalding.parquet

import org.specs._
import com.twitter.scalding._
import com.twitter.scalding.Tsv
import cascading.tuple.Fields

class ParquetSampleJob(args : Args) extends Job(args) {
  ParquetSource(Fields.ALL, args("input") )
  .read
  .write( Tsv( args("output") ) )
}

/*
//todo read part-m-00000.gz.parquet file and check we get correct data
//file has been copied from https://github.com/Parquet/parquet-mr/tree/master/parquet-cascading/src/test/resources

class ParquetSourceTest extends Specification {
  "ParquetJob" should {
    JobTest("com.twitter.scalding.parquet.ParquetSampleJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(ParquetSource(Fields.ALL, "inputFile" ), List((0, "hack hack hack and hack"))).
      sink[(Int,String)](Tsv("outputFile")){ outputBuffer =>
        val out = outputBuffer.toList
        "read correctly" in {
          Console.println(out)

          out.size must be_==(1)
        }
      }.
      run.
      finish
  }
}
*/
