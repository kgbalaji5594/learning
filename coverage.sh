curl -L https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64 > ./cc-test-reporter
chmod +x ./cc-test-reporter
./cc-test-reporter before-build
pytest --cov=./ --cov-report=xml
TEST_STATUS=$?
echo $GIT_COMMIT # only needed for debugging
GIT_COMMIT=$(git log | grep -m1 -oE '[^ ]+$')
echo $GIT_COMMIT # only needed for debugging
./cc-test-reporter after-build -t coverage.py --exit-code $TEST_STATUS || echo  ?~@~\Skipping Code Climate coverage upload?~@~]
exit $TEST_STATUS
