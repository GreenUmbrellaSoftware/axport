package com.guslabs.axport

class Column {
  Map attributes = [:]
  Map constraintsAttributes = [:]
  Column(Map attribs) {
    attributes = attribs.findAll{it.key in ['name', 'type', 'remarks']}
    constraintsAttributes = attribs.findAll{it.key in ['nullable']}
  }
}
