import time

def fetch_ip(hostname: str):
  print("Fetching details of website")
  time.sleep(1)
  print(f"The IP address of {hostname} is 192.1.1.1")
  return {"hostname": f"{hostname}" , "ip" : "192.1.1.1}

def test_ip():
  host = "www.google.com"

  results = fetch_ip(host)
  assert isinstance(results , dict)
  assert "ip" in results
  assert "hostname" in results


  assert results["hostname"] == host
