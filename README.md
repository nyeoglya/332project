# 332project
<span style="color: green;">**TEAM GREEN**</span>

**Teammate: [nyeoglya](https://github.com/nyeoglya), [zlfn](https://github.com/zlfn), [carprefer](https://github.com/carprefer)**

> For more information, please visit [**HERE**](http://pl.postech.ac.kr/~gla/cs332/index.html)

## Develpment Environment
![Scala](https://img.shields.io/badge/scala-%23DC322F.svg?style=for-the-badge&logo=scala&logoColor=white)
![Sbt](https://img.shields.io/badge/sbt-%235e150f.svg?style=for-the-badge&logo=apachenetbeanside&logoColor=white)

**JDK 21, Scala 2.13**

## Plan
**Week 1: Planning** [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week1_report.html)

**Week 2 (Midterm week): Design ideas** [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week2_report.html) / ~~[Discussion1](https://github.com/nyeoglya/332project/discussions/3) [Discussion2](https://github.com/nyeoglya/332project/discussions/4)~~ (Discussion removed accidently when cleaning github history) / [Test Code](https://github.com/nyeoglya/grpc-master-worker)

**Week 3: Overall project design** [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week3_report.html) / ~~[Comment1](https://github.com/nyeoglya/332project/discussions/3#discussioncomment-11133877) [Comment2](https://github.com/nyeoglya/332project/discussions/3#discussioncomment-11133893) [Comment3](https://github.com/nyeoglya/332project/discussions/3#discussioncomment-11133896) [Comment4](https://github.com/nyeoglya/332project/discussions/3#discussioncomment-11134228)~~

**Week 4: Create test code** [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week4_report.html) / ~~[Discussion1](https://github.com/nyeoglya/332project/discussions/7)~~ / [Image1](https://github.com/nyeoglya/332project/blob/main/report/worker_test_carprefer.png)

**Week 5: Creating physical code and testing the system** [Report](https://github.com/nyeoglya/332project/blob/main/report/week5_report.pdf)

**Week 6 (Progress Slides Deadline): Organizing Content, Preparing for Intermediate Presentation** [Presentation Slides](https://github.com/nyeoglya/332project/blob/main/presentation/intermediate/) / [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week6_report.html)

**Week 7: Project Improvement and Maintenance** [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week7_report.html)

**Week 8 (Project Deadline): Preparing for Final Presentation** [Presentation Slides](https://github.com/nyeoglya/332project/blob/main/presentation/final/) / [Report](https://htmlpreview.github.io/?https://github.com/nyeoglya/332project/blob/main/report/week8_report.html)

## How to use
You MUST login into account green.

### 1. master setting
Download ``332project`` repository by git clone. Also, download ``JDK 21``, ``master.jar``, ``worker.jar`` from github repository release.
```shell
git clone https://github.com/nyeoglya/332project.git
wget https://github.com/nyeoglya/332project/releases/download/v0.0.2/master.jar
wget https://github.com/nyeoglya/332project/releases/download/v0.0.2/worker.jar
wget https://github.com/nyeoglya/332project/releases/download/v0.0.2/jdk.tar
```

### 2. initiate
Initiate project using ``initiate``.
```sh
./332project/shellScript/initiate.sh
```

### 3. run program
Open two windows on master. Then, run ``master`` first with worker numbers.
```sh
./master [worker_num]
```

After master run properly, run ``activate_test`` with **[test_name]** (small, big, large).
```sh
./332project/shellScript/activate_test.sh [test_name]
```

According to the above method, the output is stored in the ``/home/green/dataset/[test_name]_output``.

To arbitrarily specify the input and output folder paths, you can run ``worker`` for "each" worker.
```sh
./worker [master_ip:port] -I [input_directories] -O [output_directories]
```

### 4. validation
After all sort process ends, run ``validation``, ``size_validation`` to validate whether the sorting process complete or not. Both two shell need **[test_name]**.
```sh
./332project/shellScript/validate.sh [test_name]
./332project/shellScript/size_validate.sh [test_name]
```

### 5. multi-input cases
For testing multi-input cases, run ``activate_manyDirTest`` to run worker.
```sh
./master [worker_num]
./332project/shellScript/activate_manyDirTest.sh
```

## Grading Criteria
```md
10% for forming your team by class today
30% for progress
40% for the final result
30% for correctness
15% for design, architecture, performance, implementation (subjective)
10% for the progress presentation
10% for the final presentation

We will test your code in a small cluster.
no more testing updated after the deadline.
```
