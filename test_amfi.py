import requests


def test_amfi_api():
    """Test AMFI API connectivity"""
    try:
        # Test fetching all schemes
        url = "https://api.mfapi.in/mf"
        print("Fetching all schemes...")
        response = requests.get(url, timeout=30)
        print(f"Response status: {response.status_code}")

        if response.status_code == 200:
            schemes = response.json()
            print(f"Found {len(schemes)} schemes")

            # Print first few schemes
            for i, scheme in enumerate(schemes[:5]):
                print(
                    f"  {i+1}. {scheme.get('schemeCode', 'N/A')}: {scheme.get('schemeName', 'N/A')}"
                )

            # Test fetching data for a specific scheme
            if schemes:
                scheme_code = schemes[0].get("schemeCode")
                if scheme_code:
                    print(f"\nFetching NAV history for scheme {scheme_code}...")
                    nav_url = f"https://api.mfapi.in/mf/{scheme_code}"
                    nav_response = requests.get(nav_url, timeout=30)
                    print(f"NAV response status: {nav_response.status_code}")

                    if nav_response.status_code == 200:
                        nav_data = nav_response.json()
                        print(f"NAV data keys: {nav_data.keys()}")
                        if "data" in nav_data and nav_data["data"]:
                            print(f"Sample NAV records: {len(nav_data['data'])}")
                            for i, record in enumerate(nav_data["data"][:3]):
                                print(f"  {i+1}. {record}")
        else:
            print(f"Failed to fetch schemes: {response.status_code}")

    except Exception as e:
        print(f"Error testing AMFI API: {e}")


if __name__ == "__main__":
    test_amfi_api()
