import csv
from datetime import datetime
from collections import deque

# Define the time window in seconds (adjust as needed)
TIME_WINDOW = 1.0  # 1 second

# Function to read CSV and return a list of dictionaries
def read_csv(filename):
    frames = []
    with open(filename, 'r') as open_csv_file:
        reader = csv.reader(open_csv_file)
        for row in reader:
            frame = {
                'packet_number': int(row[0].strip('"')),
                'timestamp': float(row[1].strip('"')),
                'src_ip': row[2].strip('"'),
                'dst_ip': row[3].strip('"')
            }
            frames.append(frame)
    return frames

# Read the Ping and Pong frames
ping_frames = read_csv('ping.csv')
pong_frames = read_csv('pong.csv')

# Convert Pong frames to a deque for efficient popping from the front
pong_queue = deque(pong_frames)

# List to hold the correlated pairs
correlated_frames = []

# Iterate over each Ping frame
for ping in ping_frames:
    ping_time = ping['timestamp']
    ping_src_ip = ping['src_ip']
    ping_dst_ip = ping['dst_ip']
    matched = False

    # Iterate over Pong frames to find a match
    while pong_queue:
        pong = pong_queue[0]  # Peek at the first Pong frame
        pong_time = pong['timestamp']
        pong_src_ip = pong['src_ip']
        pong_dst_ip = pong['dst_ip']

        # Check if Pong is within the time window and IPs match in reverse
        if pong_time >= ping_time and pong_time - ping_time <= TIME_WINDOW:
            if ping_src_ip == pong_dst_ip and ping_dst_ip == pong_src_ip:
                # Match found
                correlated_frames.append({
                    'ping_packet_number': ping['packet_number'],
                    'pong_packet_number': pong['packet_number'],
                    'ping_timestamp': ping_time,
                    'pong_timestamp': pong_time,
                    'ping_src_ip': ping_src_ip,
                    'ping_dst_ip': ping_dst_ip
                })
                pong_queue.popleft()  # Remove the matched Pong frame
                matched = True
                break
            else:
                # Pong frame does not match IPs, skip it
                pong_queue.popleft()
        elif pong_time - ping_time > TIME_WINDOW:
            # Pong frame is outside the time window, stop searching
            break
        else:
            # Pong frame is before the Ping frame, remove it
            pong_queue.popleft()

    if not matched:
        # No matching Pong frame found within the time window
        print(f"No Pong response for Ping packet {ping['packet_number']} within {TIME_WINDOW} seconds.")

# Output the correlated frames
print("Correlated Ping-Pong Frames:")
for pair in correlated_frames:
    print(f"Ping Packet {pair['ping_packet_number']} at {pair['ping_timestamp']} <--> "
          f"Pong Packet {pair['pong_packet_number']} at {pair['pong_timestamp']}")

# Optionally, write the correlated pairs to a CSV file
with open('correlated_ping_pong.csv', 'w', newline='') as csvfile:
    fieldnames = ['ping_packet_number', 'pong_packet_number', 'ping_timestamp', 'pong_timestamp', 'ping_src_ip', 'ping_dst_ip']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for pair in correlated_frames:
        writer.writerow(pair)
