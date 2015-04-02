name := "hnproto"

version := "1.0"

scalaVersion := "2.11.6"

import sbtprotobuf.{ProtobufPlugin=>PB}

Seq(PB.protobufSettings: _*)
