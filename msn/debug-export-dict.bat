@echo off
call pushd .

call cd D:\Git\vw-markus\vowpal_wabbit\vowpalwabbit\x64\Release

IF "%1"=="dict" GOTO dict
IF "%1"=="nodict" GOTO nodict
IF "%1"=="original" GOTO original

:dict
   @echo Using dictionary features
   @echo on
   call vw.exe -d D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string\dict.train.gz --cb_adf --rank_all --interact u‡ --cb_type mtr -l 0.005 --dictionary d:dict.features.gz --dictionary_path D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string
   @echo off
   GOTO done

:nodict
   @echo Not using dictionary features, dataset contains all features
   @echo on
   call vw.exe -d D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string\serialized-10.vw --cb_adf --rank_all --interact ud --cb_type mtr -l 0.005
   @echo off
   GOTO done

:original
   @echo Running on original serialized data
   @echo on
   call vw.exe -d D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string\train.vw.gz --cb_adf --rank_all --interact ud --cb_type mtr -l 0.005
   @echo off
   GOTO done

REM Original command line from RunTests
REM {VW} -k -c -d train-sets/dictionary_test.dat --binary --ignore w --holdout_off --passes 32 --dictionary w:dictionary_test.dict --dictionary w:dictionary_test.dict.gz --dictionary_path train-sets

:done
call popd