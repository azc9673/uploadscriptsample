import csv
import requests
import argparse

# Define a custom header to mimic a real browser.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/90.0.4430.93 Safari/537.36"
    )
}

def check_link(url):
    """
    Check if a URL is working by attempting an HTTP HEAD request with custom headers,
    and falling back to a GET request if needed.
    
    If an SSL error is encountered, try the request again with certificate verification disabled.
    
    Returns True if the link appears to work (HTTP status code < 400),
    otherwise returns False.
    """
    # Remove any extraneous whitespace
    url = url.strip()

    try:
        # Attempt a HEAD request first.
        response = requests.head(url, allow_redirects=True, timeout=10, headers=HEADERS)
        if response.status_code >= 400:
            # Some servers don't handle HEAD requests well, so try a GET request.
            response = requests.get(url, allow_redirects=True, timeout=10, headers=HEADERS)
        return response.status_code < 400

    except requests.exceptions.SSLError as ssl_err:
        print(f"SSL error for URL {url}: {ssl_err}. Trying with verify=False.")
        try:
            response = requests.get(url, allow_redirects=True, timeout=10, headers=HEADERS, verify=False)
            return response.status_code < 400
        except requests.exceptions.RequestException as e2:
            print(f"Request exception (after SSL error) for URL {url}: {e2}")
            return False

    except requests.exceptions.RequestException as req_err:
        # Print the exception details for debugging.
        print(f"Request exception for URL {url}: {req_err}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Check links (in column 7) in a CSV file and output rows with broken links."
    )
    parser.add_argument('csv_file', help="Path to the input CSV file.")
    args = parser.parse_args()

    broken_rows = []  # To store tuples of (row number, row) with broken links

    with open(args.csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        # Optionally process a header row.
        header = next(reader, None)
        if header:
            print("Header:")
            print(header)
            print("-" * 80)
            start_row = 2  # Row numbers start at 2 since header is row 1
        else:
            start_row = 1
        
        row_number = start_row
        for row in reader:
            if len(row) < 7:
                print(f"Warning: Row {row_number} does not have at least 7 columns. Skipping.")
                row_number += 1
                continue

            # Get the link from the 7th column (index 6).
            link = row[6].strip()
            if not link:
                print(f"Row {row_number} has an empty link.")
                broken_rows.append((row_number, row))
            else:
                if not check_link(link):
                    broken_rows.append((row_number, row))
            row_number += 1

    # Output the broken rows to the command line.
    if broken_rows:
        print("\nRows with broken links:")
        for row_num, row in broken_rows:
            print(f"Row {row_num}: {row}")
    else:
        print("No broken links found.")

if __name__ == '__main__':
    main()