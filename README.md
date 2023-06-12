# DBMS - Project 

Download files from the following [link](https://drive.google.com/file/d/11zlcGqaQO-SUN9Qbffju4woRL9nqb2AT/view?usp=share_link)

- Then unzip the files that you have downloaded.
- Open Terminal and create a new directory `cs167`. Refered [CS167 Big Data setup](http://github.com/aseldawy/CS167/blob/master/Labs/Lab1/CS167-Lab1.md)
- Copy paste the above downloaded files here. 
- Download the installer x64 DMG Installer (jdk-8u351-macOSx-x64.dmg) from https://www.oracle.com/java/technologies/downloads/#java8-mac. Mount the dmg file and install the app
- Set the default jdk version in terminal 
```sh
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```
- Add the following to `.zshrc` file 
```
export MAVEN_HOME="$HOME/cs167/apache-maven-3.8.7"
export HADOOP_HOME="$HOME/cs167/hadoop-3.2.3"
export SPARK_HOME="/Users/sritha/cs167/spark-3.3.1-bin-hadoop3"

PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$HADOOP_HOME/bin
```
- Download IntelliJ Community version from [IntelliJ Website](https://www.jetbrains.com/idea/download/#section=mac). 
- Select ".dmg (Intel)" or ".dmg (Apple Silicon)" according to your hardware.
- Mount the dmg file and install the app.

- Create an Empty Maven Project. 
```sh
mvn archetype:generate -DgroupId=edu.ucr.cs.cs167.sdudd001 -DartifactId=sdudd001_lab1 -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
```

- Import Your Project into InelliJ IDEA. Open IntelliJ IDEA and choose "Open". Choose the directory of your new Maven project, select the "pom.xml" file. In the promot, choose "Open as Project". The project will open. It may take some time to import the project and download neessary dependencies. Open the file "App.java" and click the small green arrow to run the main class.
- After completion of code add program parameters - Add Parameters and then run the file 


