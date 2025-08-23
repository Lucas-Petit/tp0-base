#!/bin/sh

NETWORK_NAME="tp0_testing_net"
SERVER_NAME="server"
SERVER_PORT="12345"
TEST_MESSAGE="._."

RESPONSE=$(echo -n "$TEST_MESSAGE" | docker run --rm --network="$NETWORK_NAME" -i alpine nc -w 2 "$SERVER_NAME" "$SERVER_PORT" 2>/dev/null)

if [ "$RESPONSE" = "$TEST_MESSAGE" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi
