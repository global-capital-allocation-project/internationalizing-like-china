# this file takes one argument and loads the relevent module
# it will accept as an argument any of the program names in quotes below
# it will then load the correct version and/or environment

case ${1} in 
  "stata")
    module load stata/17
    echo Loaded module: stata/17
  ;;
  "python")
    source activate gcap
    echo Loaded module: python with gcap environment
  ;;
  "R")
    module load R
    echo Loaded module: R
  ;;
  "sas")
    module load sas
    echo Loaded module: sas
  ;;
  "p7zip")
    module load system; module load p7zip
    echo Loaded module: p7zip
  ;;
  "matlab")
    module load matlab
    echo Loaded module: matlab
  ;;
esac