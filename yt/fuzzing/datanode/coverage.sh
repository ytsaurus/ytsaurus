rm -r $HOME/yt/coverage/
export LLVM_PROFILE_FILE="$HOME/yt/coverage/coverage-%p.profraw"

./yt/fuzzing/datanode/fuzztests-datanode \
    /tmp/new_inputs_datanode /tmp/datanode_corpus \
    -artifact_prefix=/tmp/fuzzing_artifacts_datanode/ -fork=10 -ignore_crashes=1 -rss_limit_mb=20000 -max_total_time=60

llvm-profdata merge -sparse $HOME/yt/coverage/coverage-*.profraw -o merged.profdata
llvm-cov show -instr-profile=merged.profdata  ./yt/fuzzing/datanode/fuzztests-datanode  > coverage.txt

# generate HTML report
# llvm-cov report ./yt/fuzzing/datanode/fuzztests-datanode -instr-profile=merged.profdata
llvm-cov show ./yt/fuzzing/datanode/fuzztests-datanode \
    -instr-profile=merged.profdata -format=html -o $HOME/yt/coverage/coverage_report/

# serve coverage report on localhost:8000
cd $HOME/yt/coverage/coverage_report/
python3 -m http.server 8000

# enable ssh-forwarding
ssh -L 8000:localhost:8000 yutsareva@158.160.26.112

# open http://localhost:8000/ in your browser
