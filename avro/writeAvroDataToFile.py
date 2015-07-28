import os
import string
import sys
from avro import schema
from avro import io
from avro import datafile
import json

avroFile="records.avro"
avroSchemaFile="avroSchema.avsc"#required to write avro files

def main():
    
    writer = open(avroFile, 'wb')
    datum_writer = io.DatumWriter()

    #reading schema from file
    schemaObject = schema.parse(open(avroSchemaFile).read())

    #schema object is required when writing avro file
    dfwriter = datafile.DataFileWriter(writer,datum_writer, schemaObject)

    #data as console input
    print("Enter employee's name and their department separated by comma")
    for line in sys.stdin.readlines():
        (employee, department) = string.split(line.strip(), ',')
        dfwriter.append({'employee':employee, 'department':department})
    dfwriter.close()

if __name__=="__main__":
    main()
