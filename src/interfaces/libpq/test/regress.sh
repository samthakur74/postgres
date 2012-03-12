#!/bin/sh
if [ -z "$PGUSER" ]; then
PGUSER=$USER
fi
if [ -z "$PGPORT" ]; then
PGPORT=5432
fi

"${BINDIR}/createdb" "$PGUSER"

echo "Running libpq URI support test..."

while read line
do
    # Frist, expand variables in the test URI line.
    uri=$(eval echo "$line")

    # But SELECT the original line, so test result doesn't depend on
    # the substituted values.
    "${BINDIR}/psql" -d "$uri" -At -c "SELECT '$line'"
done <regress.in >regress.out

if diff -c expected.out regress.out >regress.diff; then
    echo "========================================"
    echo "All tests passed"
    exit 0
else
    echo "========================================"
    echo "FAILED: the test result differs from the expected output"
    echo
    echo "Review the difference in regress.diff"
    echo "========================================"
    exit 1
fi
