# Spark Unit

## Introduction
Spark Unit is a framework for starting and stopping Zookeeper, Kafka and Spark for the purpose of unit testing of stream processing applications. Spark Unit is happy to not rely on any third-party library besides the official Kafka and Spark binaries.

## Ingredients
Spark Unit uses the following software components:

1. JDK 1.8.0

2. Scala 2.10

3. Spark Binaries

4. Kafka Binaries

5. Kafka Nine Utils

## Usage
Refer to the [unit tests](./src/test/java/info/sarihh/spark/unit).

## Maven

Spark Unit isn't hosted on [Maven Central Repository](http://search.maven.org). To use Spark Unit in your project, you need to install spark-unit-0.1.jar into your local Maven repository using the command:

```
mvn install:install-file -Dfile=spark-unit-0.1.jar -Dpackaging=jar -DgroupId=info.sarihh -DartifactId=spark-unit -Dversion=0.1
```

And then insert the following dependecny in your POM:

```XML
<dependency>
	<groupId>info.sarihh</groupId>
	<artifactId>spark-unit</artifactId>
	<version>0.1</version>
</dependency>
```

## Q&A

Post your questions to [Spark Unit mailing list](https://lists.sourceforge.net/lists/listinfo/spark-unit-list).

## Licence
Copyright &copy; **[Sari Haj Hussein](http://sarihh.info)**.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Code Disclaimer
The author of this software code has used his best efforts in preparing the code. These efforts include the development, research, testing, and optimization of the theories and programs to determine their effectiveness. This software code is not designed or intended for use in the design, construction, operation or maintenance of any nuclear facility. Author disclaims any express or implied warranty of fitness for such uses. The author makes no warranty of any kind, expressed or implied, with regard to this software code or to the documentation accompanying it. In no event shall the author be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including but not limited to, procurement of substitute goods or services; loss of use, data, or profits; or business interruption whatsoever) arising out of, the furnishing, performance, or use of this software code, even if advised of the possibilities of such damages.




