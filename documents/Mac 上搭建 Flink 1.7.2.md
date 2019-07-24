# Mac 上搭建 Flink 1.7.2 环境并构建运行简单程序入门
## 准备工作
1、安装查看 Java 的版本号，推荐使用 Java 8。
2.安装 brew

```shell
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```
> 上面方法失败的话请执行如下操作
在浏览器上进入上面的网站地址，将里面的内容复制到自己新建一个 “install.txt”里面，然后将该文件保存到任意目录，进入该目录下执行

```
ruby install.txt
```

## 安装 Flink
2、在 Mac OS X 上安装 Flink 是非常方便的。推荐通过 homebrew 来安装。

```shell
brew install apache-flink
```

3、检查安装：

```shell
kingdeMacBook-Pro:bin king$ flink --version
Version: 1.7.2, Commit ID: ceba8af
```
4、启动 flink

```shell
到该目录下执行/usr/local/Cellar/apache-flink/1.7.2/libexec/bin
./start-cluster.sh

Starting cluster.
Starting standalonesession daemon on host kingdeMacBook-Pro.local.
Starting taskexecutor daemon on host kingdeMacBook-Pro.local.
```
5、web访问

```shell
http://localhost:8081
```
## demo
1、新建一个 maven 项目
创建一个 SocketTextStreamWordCount 文件，加入以下代码：

```scala
package com.king.learn.Flink.streaming.wordcount


import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Author: king
  * @Date: 2019-02-28
  * @Desc: TODO 
  */

object SocketTextStreamWordCount {
  def main(args: Array[String]): Unit = {
    //参数检查
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
    }

    val hostname = args(0)
    val port = args(1).toInt

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val stream: DataStream[String] = env.socketTextStream(hostname, port, '\n')


    val sum = stream.flatMap(new SocketTextStreamWordCount.LineSplitter).keyBy(0).sum(1)
    sum.print

    env.execute("Socket Window WordCount")
  }

  import org.apache.flink.api.common.functions.FlatMapFunction

  final class LineSplitter extends FlatMapFunction[String, (String, Integer)] {
    def flatMap(s: String, collector: Collector[(String, Integer)]): Unit = {
      val tokens = s.toLowerCase.split("\\W+")
      for (token <- tokens) {
        if (token.length > 0) collector.collect(new Tuple2[String, Integer](token, 1))
      }
    }
  }

}

```
### maven 打包
> 注意我这是scala文件需要maven scala 打包插件

```xml
 <build>
        <!--scala待编译的文件目录-->
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <!--scala插件-->
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                        <configuration>
                            <args>
                                <!--<arg>-make:transitive</arg>--><!--scala2.11 netbean不支持这个参数-->
                                <arg>-dependencyfile</arg>
                                <arg>${project.build.directory}/.scala_dependencies</arg>
                            </args>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <!-- 这是个编译java代码的 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.0</version>
                <configuration>
                    <source>6</source>
                    <target>6</target>
                    <encoding>UTF-8</encoding>
                </configuration>
                <executions>
                    <execution>
                        <phase>compile</phase>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>


            <!-- 这是个编译scala代码的 -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.1</version>
                <executions>
                    <execution>
                        <id>scala-compile-first</id>
                        <phase>process-resources</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>


            <!--manven打包插件-->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>cn.itcast.rpc.Master</mainClass> <!--main方法-->
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```
##### 开启监听 9000 端口:

```sehll
nc -l 9000
```
##### 进入 flink 安装目录 bin 下执行以下命令跑程序：



```
./flink run -c com.king.learn.Flink.streaming.wordcount.SocketTextStreamWordCount /Volumes/mydisk/Projects/IdeaProjects/flink-learn/target/original-flink-learn-1.0-SNAPSHOT.jar 127.0.0.1 9000
```
> 注意自己的文件路径

我们可以在 webUI 中看到正在运行的程序
进入/usr/local/Cellar/apache-flink/1.7.2/libexec/log

```
tail -f flink-king-taskexecutor-0-kingdeMacBook-Pro.local.out
```
