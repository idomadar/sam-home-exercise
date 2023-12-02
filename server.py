from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import threading
import heapq
import argparse
import time
import logging

app = Flask(__name__)

# Debugging
DEBUG = False
LEN_SLAVES = 10
MINIMUM_RANGE = 101
MAXIMUM_RANGE = 101 + LEN_SLAVES
# IP pool
available_slaves = ['192.168.0.{}'.format(i) for i in range(MINIMUM_RANGE, MAXIMUM_RANGE)]
unavailable_slaves = []
lock = threading.Lock()


@app.route('/get_slaves')
def get_slaves():
    try:
        # Parse the amount and duration from the GET request
        amount = int(request.args.get('amount'))
        duration = int(request.args.get('duration'))

        if amount <= 0 or duration <= 0 or amount > LEN_SLAVES:
            error_message = "Invalid amount or duration"
            logging.warning(error_message)
            return jsonify({"error": error_message}), 400

        with lock:
            if amount > len(available_slaves):
                # Calculate the time needed for the remaining slaves
                difference = amount - len(available_slaves)
                wait_time = calculate_wait_time(difference)
                logging.warning(f"Not enough slaves, come back in {wait_time} seconds")
                result = {"slaves": [], "come_back": wait_time}
            else:
                result = {"slaves": available_slaves[:amount]}
                for ip in result['slaves']:
                    available_slaves.remove(ip)
                    release_time = datetime.now() + timedelta(seconds=duration)
                    # Push slave to heap based on release time
                    heapq.heappush(unavailable_slaves, (release_time, ip))
                # Start a timer to release slaves
                threading.Timer(duration, release_slaves, [amount]).start()
        if DEBUG:
            logging.warning(f"Result: {result}")
            logging.warning(f"Available slaves: {available_slaves}")
            logging.warning(f"Unavailable slaves: {unavailable_slaves}")
        return jsonify(result)
    except ValueError as e:
        error_message = f"Error: {str(e)}"
        logging.warning(error_message)

        # Returning an error response
        error_response = {
            'status': 'error',
            'message': error_message
        }
        # HTTP status code 400 for Bad Request
        return jsonify(error_response), 400


def release_slaves(amount):
    with lock:
        for _ in range(amount):
            release_time, ip = heapq.heappop(unavailable_slaves)
            # Ensure that the slave has reached its release time
            while datetime.now() < release_time:
                time.sleep(1)
            available_slaves.append(ip)


def calculate_wait_time(amount):
    return abs((datetime.now() - unavailable_slaves[amount - 1][0]).total_seconds())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='HTTP Server that manages a pool of 10 slaves, where each slave has its own IP address')
    parser.add_argument('--port', type=int, default=8080, help='Port number for the server')
    parser.add_argument('--debug', type=bool, default=False,
                        help='If flags is True, we print debugging values in the server logs')
    args = parser.parse_args()

    # Running the Flask app with the specified port
    DEBUG = args.debug
    logging.warning(f"Running the server on port: {args.port}, and debug option is: {args.debug}")
    app.run(port=args.port)
