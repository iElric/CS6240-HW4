Hadoop MapReduce FollowerCount

Code author
-----------
Yexin Wang

Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.11.12
- Hadoop 2.9.2
- Spark 2.3.4 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Platform
-----------
Mac

Environment
-----------
1) Example ~/.bash_profiles(Or ~./zshrc if you install zsh):
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home
export HADOOP_HOME=/Users/wangyexin/Apache/hadoop-2.9.2
export SCALA_HOME=/Users/wangyexin/Apache/scala-2.11.12
export SPARK_HOME=/Users/wangyexin/Apache/spark-2.3.4-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
8) Download log file(create a log folder under current project folder first)
   aws s3 sync s3://your_bucket_name/log log
9) Running in IDE
   configure the run configuration with input and output path, set the local master in VM option
