Using Scalable partitioned data to find statistics on a column by a grouping variable

Related to:

see
https://goo.gl/6mSUZ6
https://stackoverflow.com/questions/47255408/sas-whats-the-optimal-way-to-find-the-sum-of-a-column-by-another-column


Benchmarks (this is worst case, very poor locality of reference, because groups are randomly located)
           ( I/O dominates so it is hard to see a massive improvement)

   1. 86 seconds -> One task running proc summary
   2. 52 seconds -> Multiprogramming 5 proc summary tasks with partitioned data


Problem: I have a small table with 400,000,000 million observations. I want to calculate
The statistics below for each of 20 groups. I would like to do it in less than a minute on my
old $100 laptop ie Dell 6420, cica 2012.

Desired statistics by group

  CSS
  Range
  CV
  Skewness
  Kurtosis
  Stddev
  Lclm
  Stderr
  Max
  Sum
  Mean
  Min
  Uclm
  Mode
  USS
  Q1
  MEDIAN
  Q3
  P5
  P95
  P99

  Steps
  =====
    1. Partition the input data into 24 partitions over two channels (2 drives).
       C drive and a second D drive. D drive is in the laptop optical drive bay.
    2. Create a macro which is able to selects mutually exclusive sets of groups.
    3. Setp up five separate parallel tasks that call the macro


INPUT
=====

    SPDE.HAVE total obs=400,000,000

          Obs    ID    VALUE

            1    16     321
            2    04     907
            3    08     222
            4    16     399
          ....

    Partitioned data

    C drive

      c:/wrk/spde_c/
            have.dpf.0ae70b24.1.1.spds9    ** 12 of these
            have.dpf.0ae70b24.3.1.spds9
          ...
            have.dpf.0ae70b24.23.1.spds9

    D drive (Optical CD bay)

             have.dpf.0ae70b24.0.1.spds9    ** 12 of these
             have.dpf.0ae70b24.2.1.spds9
           ...
             have.dpf.0ae70b24.24.1.spds9

    There are more compelling reasons to partition data, ie complex parallel indexing, big
    data (ie single tables over 1TB, 32 simultaneous processes.
    This is not big data.


WORKING CODE (five simultaneous 'batch' jobs)
==============================================

    proc summary data=spde.have(where=("&min" <= id <= "&max"))
       NMISS CSS RANGE CV SKEWNESS KURTOSIS STDDEV LCLM
       Q1 MEDIAN Q3 P5 P95 P99
       STDERR MAX SUM MEAN MIN UCLM MODE USS nway;
      class id;
      var value;
     output out=sd1.meany(drop=_type_)
          NMISS= CSS= RANGE= CV= SKEWNESS= KURTOSIS=
          STDDEV= LCLM= STDERR= MAX= SUM= MEAN= MIN=
          Q1= MEDIAN= Q3= P5= P95= P95= P99=
          UCLM= MODE= USS= / autoname


    systask command "&_s -termstmt %nrstr(%meany(min=01,max=04);) -log d:\log\a1.log" taskname=sys1;
    systask command "&_s -termstmt %nrstr(%meany(min=05,max=08);) -log d:\log\a2.log" taskname=sys2;
    systask command "&_s -termstmt %nrstr(%meany(min=09,max=12);) -log d:\log\a3.log" taskname=sys3;
    systask command "&_s -termstmt %nrstr(%meany(min=13,max=16);) -log d:\log\a4.log" taskname=sys4;
    systask command "&_s -termstmt %nrstr(%meany(min=17,max=20);) -log d:\log\a5.log" taskname=sys5;

OUTPUT (20 records one per group)
==================================

    This is one of the 20 groups (group= '10')

    Middle Observation(10 ) of combine - Total Obs 20

     -- CHARACTER --               Type Length    Sample Value
    ID                               C    2       10      *** group=10

     -- NUMERIC --
    _FREQ_                           N    8       20002378
    VALUE_NMISS                      N    8       0
    VALUE_CSS                        N    8       1.6664445E12
    VALUE_RANGE                      N    8       999
    VALUE_CV                         N    8       57.660349133
    VALUE_SKEW                       N    8       -0.000375401
    VALUE_KURT                       N    8       -1.199555952
    VALUE_STDDEV                     N    8       288.63873792
    VALUE_LCLM                       N    8       500.45795199
    VALUE_STDERR                     N    8       0.0645377472
    VALUE_MAX                        N    8       1000
    VALUE_SUM                        N    8       10012879263
    VALUE_MEAN                       N    8       500.58444366
    VALUE_MIN                        N    8       1
    VALUE_Q1                         N    8       251
    VALUE_MEDIAN                     N    8       501
    VALUE_Q3                         N    8       751
    VALUE_P5                         N    8       51
    VALUE_P95                        N    8       951
    VALUE_P99                        N    8       991
    VALUE_UCLM                       N    8       500.71093533
    VALUE_MODE                       N    8       928
    VALUE_USS                        N    8       6.678736E12

*                _               _       _
 _ __ ___   __ _| | _____     __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

;

libname sd1 "d:/sd1";

libname spde spde
 ('c:\wrk\spde_c')
    metapath =('c:\wrk\spde_c\metadata')
    indexpath=(
          'c:\wrk\spde_c'
          ,'d:\wrk\spde_d')
    datapath =(
          'c:\wrk\spde_c'
          ,'d:\wrk\spde_d')
    partsize=250m
;

proc datasets lib=sd1 kill;
run;quit;
proc datasets lib=spde kill;

data spde.have (drop=rownum);
  length id $2;
  do rownum = 1 to 400000000;
    id = put(ceil(20*ranuni(123)),z2.);
    value = ceil(1000*ranuni(312));
    output;
  end;
run;quit;

* you need a macro variable with SAS executable with appropriate options;
* the config file needs to set up properly;

%let _S=\PROGRA~1\SASHome\SASFoundation\9.4\sas.exe -sysin nul
     -log nul -work \wrk -rsasuser -autoexec \oto\tut_Oto.sas
     -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg;

%put &=_s;

* Config options for Dell I7 with 8gb?;

-BUFSIZE 64K
-UBUFSIZE 64K
-IBUFSIZE 32767
-BUFNO 10
-IBUFNO 10
-UBUFNO 10

-SET SAS_NO_RANDOM_ACCESS "1"

-MEMSIZE 7.9g
-SORTSIZE 2g
-realmemsize 8g


*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;

*

* put macro in autocall library so it is easy to call;
data _null_;file "c:\oto\meany.sas" lrecl=512;input;put _infile_;putlog _infile_;
cards4;
%macro meany(min=,max=);

   libname spde spde
    ('c:\wrk\spde_c')
    metapath =('c:\wrk\spde_c\metadata')
    indexpath=(
        'c:\wrk\spde_c'
       ,'d:\wrk\spde_d')
    datapath =(
        'c:\wrk\spde_c'
       ,'d:\wrk\spde_d')
    partsize=250m;
   ;

   libname sd1 "d:/sd1";

    proc summary data=spde.have(where=("&min" <= id <= "&max"))
       NMISS CSS RANGE CV SKEWNESS KURTOSIS STDDEV LCLM
       Q1 MEDIAN Q3 P5 P99
       STDERR MAX SUM MEAN MIN UCLM MODE USS nway;
      class id;
      var value;
      output out=sd1.meany&min.(drop=_type_)
          NMISS= CSS= RANGE= CV= SKEWNESS= KURTOSIS=
          STDDEV= LCLM= STDERR= MAX= SUM= MEAN= MIN=
          Q1= MEDIAN= Q3= P5= P95= P99=
          UCLM= MODE= USS= / autoname;
    run;quit;

%mend meany;
;;;;
run;quit;


/* you can run interactively to check macro */

%let tym=%sysfunc(time());
systask kill sys1 sys2 sys3 sys4 sys5;
systask command "&_s -termstmt %nrstr(%meany(min=01,max=04);) -log d:\log\a1.log" taskname=sys1;
systask command "&_s -termstmt %nrstr(%meany(min=05,max=08);) -log d:\log\a2.log" taskname=sys2;
systask command "&_s -termstmt %nrstr(%meany(min=09,max=12);) -log d:\log\a3.log" taskname=sys3;
systask command "&_s -termstmt %nrstr(%meany(min=13,max=16);) -log d:\log\a4.log" taskname=sys4;
systask command "&_s -termstmt %nrstr(%meany(min=17,max=20);) -log d:\log\a5.log" taskname=sys5;
waitfor sys1 sys2 sys3 sys4 sys5;
%put %sysevalf( %sysfunc(time()) - &tym);

/*

2952  %let tym=%sysfunc(time());
NOTE: "sys3" is not an active task/transaction.
NOTE: "sys4" is not an active task/transaction.
2953  systask kill sys1 sys2 sys3 sys4 sys5;
2954  systask command "&_s -termstmt %nrstr(%meany(min=01,max=04);) -log d:\lo
SYMBOLGEN:  Macro variable _S resolves to \PROGRA~1\SASHome\SASFoundation\9.4\
            -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg
2955  systask command "&_s -termstmt %nrstr(%meany(min=05,max=08);) -log d:\lo
SYMBOLGEN:  Macro variable _S resolves to \PROGRA~1\SASHome\SASFoundation\9.4\
            -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg
2956  systask command "&_s -termstmt %nrstr(%meany(min=09,max=12);) -log d:\lo
SYMBOLGEN:  Macro variable _S resolves to \PROGRA~1\SASHome\SASFoundation\9.4\
            -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg
2957  systask command "&_s -termstmt %nrstr(%meany(min=13,max=16);) -log d:\lo
SYMBOLGEN:  Macro variable _S resolves to \PROGRA~1\SASHome\SASFoundation\9.4\
            -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg
2958  systask command "&_s -termstmt %nrstr(%meany(min=17,max=20);) -log d:\lo
SYMBOLGEN:  Macro variable _S resolves to \PROGRA~1\SASHome\SASFoundation\9.4\
            -nosplash -sasautos \oto -RLANG -config \cfg\sasv9.cfg
2959  waitfor sys1 sys2 sys3 sys4 sys5;
NOTE: Task "sys4" produced no LOG/Output.
2960  %put %sysevalf( %sysfunc(time()) - &tym);
SYMBOLGEN:  Macro variable TYM resolves to 72312.4119999408


52.3540000915964 (elapsed time)

*/

* put the pieces together;
data combine;
  set sd1.meany:;
run;quit;

real time           0.04 seconds
user cpu time       0.00 seconds
system cpu time     0.00 seconds

* single task and no partitions ;
data have (drop=rownum);
  length id $2;
  do rownum = 1 to 400000000;
    id = put(ceil(20*ranuni(123)),z2.);
    value = ceil(1000*ranuni(312));
    output;
  end;
run;quit;

* 47 seconds;

proc summary data=have
   NMISS CSS RANGE CV SKEWNESS KURTOSIS STDDEV LCLM
   Q1 MEDIAN Q3 P5 P99
   STDERR MAX SUM MEAN MIN UCLM MODE USS nway;
  class id;
  var value;
  output out=meany(drop=_type_)
      NMISS= CSS= RANGE= CV= SKEWNESS= KURTOSIS=
      STDDEV= LCLM= STDERR= MAX= SUM= MEAN= MIN=
      Q1= MEDIAN= Q3= P5= P95= P99=
      UCLM= MODE= USS= / autoname;
run;quit;

NOTE: There were 400000000 observations read from the data set WORK.HAVE.
NOTE: The data set WORK.MEANY has 20 observations and 24 variables.
NOTE: PROCEDURE SUMMARY used (Total process time):
real time           1:25.74
user cpu time       3:07.49
system cpu time     4.29 seconds

