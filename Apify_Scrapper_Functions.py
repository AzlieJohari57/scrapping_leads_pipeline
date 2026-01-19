
import pandas as pd
import time
from fuzzywuzzy import fuzz, process
import re
from apify_client import ApifyClient
import requests
from urllib.parse import urlparse


def Get_Phone_Number_From_Facebook(df):

    facebook_only_df = df.copy()
    facebook_only_df = facebook_only_df[facebook_only_df["Facebook"].notna()]

    def clean_facebook(x):
        if isinstance(x, list):
            return x[0] if len(x) > 0 else None
        elif pd.isna(x):
            return None
        else:
            return x

    def clean_facebook_url(url):
        """Clean and validate Facebook URL for the scraper."""
        if not url or pd.isna(url):
            return None

        url = str(url).strip()

        # Skip personal profiles - Actor only works with business pages
        if 'profile.php' in url:
            return None

        # Remove /about or /about_profile_transparency suffixes
        url = re.sub(r'/about.*$', '', url)

        # Ensure it's a Facebook URL
        if 'facebook.com' not in url.lower():
            return None

        return url

    facebook_only_df["Facebook"] = facebook_only_df["Facebook"].apply(clean_facebook)
    facebook_only_df["Facebook_Cleaned"] = facebook_only_df["Facebook"].apply(clean_facebook_url)

    # Separate valid URLs from invalid/filtered ones
    valid_facebook_df = facebook_only_df[facebook_only_df["Facebook_Cleaned"].notna() & (facebook_only_df["Facebook_Cleaned"] != "")].copy()
    invalid_facebook_df = facebook_only_df[facebook_only_df["Facebook_Cleaned"].isna() | (facebook_only_df["Facebook_Cleaned"] == "")].copy()

    # Use cleaned URLs for processing
    valid_facebook_df["Facebook"] = valid_facebook_df["Facebook_Cleaned"]
    valid_facebook_df = valid_facebook_df.drop(columns=["Facebook_Cleaned"])
    invalid_facebook_df = invalid_facebook_df.drop(columns=["Facebook_Cleaned"])

    print(f"Valid Facebook URLs: {len(valid_facebook_df)}, Filtered out (profiles/invalid): {len(invalid_facebook_df)}")

    client = ApifyClient("")

    BATCH_SIZE = 500
    MAX_CONCURRENCY = 3

    def validate_singapore_number(phone):
        """Validates and standardizes Singapore phone numbers."""
        if not phone:
            return None
        cleaned = re.sub(r'[\s\-\(\)\.\|/]', '', str(phone))
        if cleaned.startswith('+'):
            cleaned = cleaned[1:]
        if cleaned.startswith('65'):
            number_part = cleaned[2:]
            if re.match(r'^[689]\d{7}$', number_part):
                return f"+65{number_part}"
        elif re.match(r'^[689]\d{7}$', cleaned):
            return f"+65{cleaned}"
        return None


    def run_facebook_scraper_batch(client, facebook_urls_batch):
        """Run Apify Facebook scraper for a batch of URLs."""

        run_input = {
            "pages": facebook_urls_batch,
            "language": "en-US",
        }

        try:
            run = client.actor("oJ48ceKNY7ueGPGL0").call(run_input=run_input)

            results = []
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)

            return results, None

        except Exception as e:
            return [], str(e)


    if len(valid_facebook_df) > 0:
        print(f"Processing {len(valid_facebook_df)} Facebook pages...")

        total_phones_found = 0
        total_rows = len(valid_facebook_df)
        num_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(0, total_rows, BATCH_SIZE):
            batch = valid_facebook_df.iloc[batch_idx:batch_idx + BATCH_SIZE]
            facebook_urls = [str(row['Facebook']).strip() for _, row in batch.iterrows()]
            batch_indices = list(batch.index)

            print(f"Batch {(batch_idx//BATCH_SIZE)+1}/{num_batches}...")

            # Remove duplicates while preserving order and tracking original indices
            unique_urls = []
            seen = set()
            for url in facebook_urls:
                if url not in seen:
                    unique_urls.append(url)
                    seen.add(url)

            print(f"  Original URLs: {len(facebook_urls)}, Unique URLs: {len(unique_urls)}")

            items, error = run_facebook_scraper_batch(client, unique_urls)

            if error:
                print(f"  ERROR: {error}")
                for idx in batch_indices:
                    valid_facebook_df.loc[idx, 'Phones'] = None
                continue

            print(f"  Retrieved {len(items)} results from Apify")
            if len(items) > 0:
                print(f"  Sample item keys: {list(items[0].keys())[:10]}")

            url_to_item = {}
            for item in items:
                fb_url = item.get('facebookUrl') or item.get('url') or item.get('pageUrl')
                if fb_url:
                    normalized_url = fb_url.lower().strip().rstrip('/')
                    url_to_item[normalized_url] = item

            for idx, row in batch.iterrows():
                original_url = str(row['Facebook']).strip()
                normalized_search = original_url.lower().strip().rstrip('/')

                item = url_to_item.get(normalized_search)

                if item:
                    raw_phone = item.get('phone', None) or item.get('wa_number', None) or item.get('mobile', None)
                    print(f"  DEBUG: URL {original_url[:40]}... | raw_phone={raw_phone}")
                    phone = validate_singapore_number(raw_phone)

                    if phone:
                        valid_facebook_df.loc[idx, 'Phones'] = phone
                        total_phones_found += 1
                    else:
                        valid_facebook_df.loc[idx, 'Phones'] = None
                else:
                    valid_facebook_df.loc[idx, 'Phones'] = None

            if batch_idx + BATCH_SIZE < total_rows:
                time.sleep(3)

        print(f"Done! Found {total_phones_found}/{total_rows} phone numbers.")
    else:
        print("No valid Facebook URLs to process.")
        valid_facebook_df["Phones"] = None

    df_with_phones = valid_facebook_df[valid_facebook_df["Phones"].notna()]
    df_without_phones = valid_facebook_df[valid_facebook_df["Phones"].isna()]

    final_df_1 = df_with_phones[df_with_phones["Phones"].duplicated(keep=False) == False]
    refilter_df_1 = df_with_phones[df_with_phones["Phones"].duplicated(keep=False) == True]

    # Include filtered/invalid URLs in the no-phone list
    df_without_phones_2 = pd.concat([refilter_df_1, df_without_phones, invalid_facebook_df], ignore_index=True)

    print(f"Summary: {len(final_df_1)} with phones, {len(df_without_phones_2)} without phones (includes {len(invalid_facebook_df)} filtered URLs)")

    return final_df_1, df_without_phones_2


def verify_website_accessibility(url, timeout=10):
    """
    Verify if a website is accessible via HTTP/HTTPS.
    Returns: ('yes', final_url) if accessible, ('no', error_reason) if not
    """
    if not url or pd.isna(url) or str(url).strip() == "":
        return ('no', 'Empty URL')

    url = str(url).strip()

    # Ensure URL has a scheme
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        # Try HTTPS first
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        if response.status_code < 400:
            return ('yes', response.url)

        # If HTTPS fails with 4xx/5xx, try HTTP
        if url.startswith('https://'):
            http_url = url.replace('https://', 'http://', 1)
            response = requests.head(http_url, timeout=timeout, allow_redirects=True)
            if response.status_code < 400:
                return ('yes', response.url)

        return ('no', f'HTTP {response.status_code}')

    except requests.exceptions.SSLError:
        # SSL error with HTTPS, try HTTP
        try:
            if url.startswith('https://'):
                http_url = url.replace('https://', 'http://', 1)
                response = requests.head(http_url, timeout=timeout, allow_redirects=True)
                if response.status_code < 400:
                    return ('yes', response.url)
            return ('no', 'SSL Error')
        except Exception as e:
            return ('no', f'SSL Error: {str(e)[:50]}')

    except requests.exceptions.Timeout:
        return ('no', 'Timeout')

    except requests.exceptions.ConnectionError:
        return ('no', 'Connection Failed')

    except requests.exceptions.TooManyRedirects:
        return ('no', 'Too Many Redirects')

    except Exception as e:
        return ('no', f'Error: {str(e)[:50]}')


def Get_Phone_Number_From_Website(df):

    RecordOwl_Leads = df.copy()

    # --- Initialize Apify client ---
    client = ApifyClient("")

    # COST-OPTIMIZED BATCH CONFIGURATION
    BATCH_SIZE = 500         # Increased batch size for fewer actor runs
    MAX_CONCURRENCY = 3      # Increased concurrency for faster completion
    MAX_RETRIES = 1          # Reduced retries (saves compute on failures)
    PAGE_TIMEOUT = 15        # Reduced timeout from 30s
    FUNCTION_TIMEOUT = 30    # Reduced timeout from 60s

    # Keywords to skip scraping
    SKIP_KEYWORDS = ["mycareersfuture", "recordowl", "bizfile"]

    def create_website_scraper_pagefunction():
        """Optimized pageFunction for extracting phone numbers - extracts from current page then tries contact page"""
        return """
    async function pageFunction(context) {
        const { page, log, request } = context;
        const website = request.url;
        const isContact = request.userData?.isContact || false;
        const isHomepage = request.userData?.isHomepage || false;

        log.info(`🔍 Scraping: ${website}`);

        try {
            // STEP 1: Extract phone numbers from current page FIRST (critical fix)
            // This ensures we capture data from both homepage and contact pages
            await page.waitForSelector('body', { timeout: 5000 });

            const contactData = await page.evaluate(() => {
                function formatSingaporePhone(text) {
                    if (!text) return null;

                    const digitsOnly = text.replace(/\\D/g, '');

                    // CRITICAL: Check for NON-SINGAPORE country codes FIRST and REJECT immediately
                    // List of country codes to explicitly reject (2-digit and 3-digit codes)
                    const bannedCountryCodes = [
                        '1',   // US/Canada
                        '7',   // Russia/Kazakhstan
                        '20',  // Egypt
                        '27',  // South Africa
                        '30',  // Greece
                        '31',  // Netherlands
                        '32',  // Belgium
                        '33',  // France
                        '34',  // Spain
                        '36',  // Hungary
                        '39',  // Italy
                        '40',  // Romania
                        '41',  // Switzerland
                        '43',  // Austria
                        '44',  // UK
                        '45',  // Denmark
                        '46',  // Sweden
                        '47',  // Norway
                        '48',  // Poland
                        '49',  // Germany
                        '51',  // Peru
                        '52',  // Mexico
                        '53',  // Cuba
                        '54',  // Argentina
                        '55',  // Brazil
                        '56',  // Chile
                        '57',  // Colombia
                        '58',  // Venezuela
                        '60',  // Malaysia
                        '61',  // Australia
                        '62',  // Indonesia
                        '63',  // Philippines
                        '64',  // New Zealand
                        '66',  // Thailand
                        '81',  // Japan
                        '82',  // South Korea
                        '84',  // Vietnam
                        '86',  // China
                        '90',  // Turkey
                        '91',  // India
                        '92',  // Pakistan
                        '93',  // Afghanistan
                        '94',  // Sri Lanka
                        '95',  // Myanmar
                        '98',  // Iran
                        '212', // Morocco
                        '213', // Algeria
                        '216', // Tunisia
                        '220', // Gambia
                        '234', // Nigeria
                        '254', // Kenya
                        '255', // Tanzania
                        '260', // Zambia
                        '351', // Portugal
                        '352', // Luxembourg
                        '353', // Ireland
                        '354', // Iceland
                        '358', // Finland
                        '370', // Lithuania
                        '371', // Latvia
                        '372', // Estonia
                        '373', // Moldova
                        '374', // Armenia
                        '375', // Belarus
                        '376', // Andorra
                        '377', // Monaco
                        '378', // San Marino
                        '380', // Ukraine
                        '381', // Serbia
                        '382', // Montenegro
                        '385', // Croatia
                        '386', // Slovenia
                        '387', // Bosnia
                        '389', // North Macedonia
                        '420', // Czech Republic
                        '421', // Slovakia
                        '423', // Liechtenstein
                        '500', // Falkland Islands
                        '501', // Belize
                        '502', // Guatemala
                        '503', // El Salvador
                        '504', // Honduras
                        '505', // Nicaragua
                        '506', // Costa Rica
                        '507', // Panama
                        '508', // Saint Pierre
                        '509', // Haiti
                        '590', // Guadeloupe
                        '591', // Bolivia
                        '592', // Guyana
                        '593', // Ecuador
                        '594', // French Guiana
                        '595', // Paraguay
                        '596', // Martinique
                        '597', // Suriname
                        '598', // Uruguay
                        '599', // Netherlands Antilles
                        '670', // East Timor
                        '672', // Antarctica
                        '673', // Brunei
                        '674', // Nauru
                        '675', // Papua New Guinea
                        '676', // Tonga
                        '677', // Solomon Islands
                        '678', // Vanuatu
                        '679', // Fiji
                        '680', // Palau
                        '681', // Wallis and Futuna
                        '682', // Cook Islands
                        '683', // Niue
                        '685', // Samoa
                        '686', // Kiribati
                        '687', // New Caledonia
                        '688', // Tuvalu
                        '689', // French Polynesia
                        '690', // Tokelau
                        '691', // Micronesia
                        '692', // Marshall Islands
                        '850', // North Korea
                        '852', // Hong Kong - THE ONE CAUSING YOUR ISSUE!!!
                        '853', // Macau
                        '855', // Cambodia
                        '856', // Laos
                        '880', // Bangladesh
                        '886', // Taiwan
                        '960', // Maldives
                        '961', // Lebanon
                        '962', // Jordan
                        '963', // Syria
                        '964', // Iraq
                        '965', // Kuwait
                        '966', // Saudi Arabia
                        '967', // Yemen
                        '968', // Oman
                        '970', // Palestine
                        '971', // UAE
                        '972', // Israel
                        '973', // Bahrain
                        '974', // Qatar
                        '975', // Bhutan
                        '976', // Mongolia
                        '977', // Nepal
                    ];

                    // STEP 1: Validate length first (must be 8 or 10 digits only)
                    if (digitsOnly.length !== 8 && digitsOnly.length !== 10) {
                        return null; // Invalid length
                    }

                    // STEP 2: For 10-digit numbers, check for banned country codes FIRST
                    if (digitsOnly.length === 10) {
                        // Extract first 2-3 digits to check country code
                        const first2 = digitsOnly.substring(0, 2);
                        const first3 = digitsOnly.substring(0, 3);

                        // Check against banned codes (optimized - check most common first)
                        if (first3 === '852' || first3 === '853' || first3 === '886' || first3 === '855' ||
                            first3 === '856' || first3 === '850' || first3 === '880') {
                            return null; // Banned 3-digit country code (Hong Kong, etc.)
                        }

                        if (first2 === '1' || first2 === '7' || first2 === '60' || first2 === '61' ||
                            first2 === '62' || first2 === '63' || first2 === '64' || first2 === '66' ||
                            first2 === '81' || first2 === '82' || first2 === '84' || first2 === '86' ||
                            first2 === '90' || first2 === '91' || first2 === '92' || first2 === '93' ||
                            first2 === '94' || first2 === '95' || first2 === '98' || first2 === '20' ||
                            first2 === '27' || first2 === '30' || first2 === '31' || first2 === '32' ||
                            first2 === '33' || first2 === '34' || first2 === '36' || first2 === '39' ||
                            first2 === '40' || first2 === '41' || first2 === '43' || first2 === '44' ||
                            first2 === '45' || first2 === '46' || first2 === '47' || first2 === '48' ||
                            first2 === '49' || first2 === '51' || first2 === '52' || first2 === '53' ||
                            first2 === '54' || first2 === '55' || first2 === '56' || first2 === '57' ||
                            first2 === '58') {
                            return null; // Banned 2-digit country code
                        }

                        // MUST be Singapore (65)
                        if (first2 !== '65') {
                            return null; // Not Singapore
                        }

                        // Validate Singapore number format
                        const number = digitsOnly.substring(2);
                        if (/^[689]\\d{7}$/.test(number)) {
                            return '+65' + number;
                        }
                        return null;
                    }

                    // STEP 3: For 8-digit numbers, validate Singapore format only
                    if (digitsOnly.length === 8) {
                        if (/^[689]\\d{7}$/.test(digitsOnly)) {
                            return '+65' + digitsOnly;
                        }
                        return null;
                    }

                    return null;
                }

                const phones = [];
                const phoneSet = new Set();

                // Method 1: Extract from tel: links (most reliable)
                document.querySelectorAll('a[href^="tel:"], a[href*="tel:"]').forEach(a => {
                    const telHref = a.href.replace(/^tel:/, '').trim();
                    const formatted = formatSingaporePhone(telHref);
                    if (formatted && !phoneSet.has(formatted)) {
                        phones.push(formatted);
                        phoneSet.add(formatted);
                    }
                });

                // Method 2: Extract from WhatsApp links
                document.querySelectorAll('a[href*="wa.me"], a[href*="whatsapp"], a[href*="api.whatsapp"]').forEach(a => {
                    const match = a.href.match(/\\d+/g);
                    if (match) {
                        const number = match.join('');
                        const formatted = formatSingaporePhone(number);
                        if (formatted && !phoneSet.has(formatted)) {
                            phones.push(formatted);
                            phoneSet.add(formatted);
                        }
                    }
                });

                // Method 3: Extract from structured data (Schema.org)
                document.querySelectorAll('script[type="application/ld+json"]').forEach(script => {
                    try {
                        const data = JSON.parse(script.textContent);
                        const extractPhone = (obj) => {
                            if (obj && typeof obj === 'object') {
                                if (obj.telephone || obj.phone) {
                                    const formatted = formatSingaporePhone(obj.telephone || obj.phone);
                                    if (formatted && !phoneSet.has(formatted)) {
                                        phones.push(formatted);
                                        phoneSet.add(formatted);
                                    }
                                }
                                Object.values(obj).forEach(extractPhone);
                            }
                        };
                        extractPhone(data);
                    } catch (e) {}
                });

                // Method 4: Pattern matching in visible text (STRICT Singapore-only patterns)
                const bodyText = document.body.innerText || document.body.textContent || '';
                const phonePatterns = [
                    // Pattern 1: +65 format (MOST RELIABLE - explicit Singapore)
                    /\\+65[\\s\\-\\.]?[689]\\d{3}[\\s\\-\\.]?\\d{4}/g,
                    // Pattern 2: Parentheses format: (+65) 9123 4567
                    /\\(\\+65\\)[\\s\\-\\.]?[689]\\d{3}[\\s\\-\\.]?\\d{4}/g,
                    // Pattern 3: Space/dash separated: 65 9123-4567 or 65-9123-4567
                    /\\b65[\\s\\-][689]\\d{3}[\\s\\-\\.]?\\d{4}\\b/g,
                    // Pattern 4: REMOVED - Too dangerous, matches partial foreign numbers
                    // Old: /(?<!\\d)[689]\\d{3}[\\s\\-\\.]\\d{4}(?!\\d)/g
                    // This was matching "6150-9118" from "(+852) 6150-9118"
                ];

                phonePatterns.forEach(pattern => {
                    const matches = bodyText.matchAll(pattern);
                    for (const match of matches) {
                        const formatted = formatSingaporePhone(match[0]);
                        if (formatted && !phoneSet.has(formatted)) {
                            phones.push(formatted);
                            phoneSet.add(formatted);
                        }
                    }
                });

                // Method 5: Extract from meta tags
                document.querySelectorAll('meta[property*="phone"], meta[name*="phone"]').forEach(meta => {
                    const content = meta.getAttribute('content');
                    if (content) {
                        const formatted = formatSingaporePhone(content);
                        if (formatted && !phoneSet.has(formatted)) {
                            phones.push(formatted);
                            phoneSet.add(formatted);
                        }
                    }
                });

                // FINAL SAFETY CHECK: Verify all collected phones are valid Singapore numbers
                const validatedPhones = phones.filter(phone => {
                    // Must be in format +65XXXXXXXX where X starts with 6, 8, or 9
                    return phone && phone.match(/^\\+65[689]\\d{7}$/);
                });

                return { phones: validatedPhones };
            });

            // STEP 2: If on homepage and no phones found, try to find and enqueue contact page
            // If phones were found, still try contact page for additional numbers
            if (!isContact && !isHomepage) {
                const contactUrl = await page.evaluate(() => {
                    // Enhanced contact page detection - look for common patterns
                    const contactKeywords = [
                        'contact', 'contacts', 'contact-us', 'contactus', 'reach-us',
                        'get-in-touch', 'enquiry', 'enquiries', 'reach-out',
                        'connect', 'talk-to-us', 'support'
                    ];

                    // Find contact links
                    const links = Array.from(document.querySelectorAll('a[href]'));
                    for (const link of links) {
                        const href = link.href.toLowerCase();
                        const text = (link.textContent || '').toLowerCase().trim();

                        // Check if link contains contact keywords
                        if (contactKeywords.some(kw => href.includes(kw) || text.includes(kw))) {
                            // Make sure it's not external or social media
                            const currentDomain = window.location.hostname.replace('www.', '');
                            try {
                                const linkDomain = new URL(link.href).hostname.replace('www.', '');
                                if (linkDomain === currentDomain || linkDomain.endsWith(currentDomain)) {
                                    return link.href;
                                }
                            } catch (e) {
                                // Relative URL, safe to use
                                return link.href;
                            }
                        }
                    }
                    return null;
                });

                if (contactUrl && contactUrl !== website) {
                    // Enqueue contact page for scraping, but DON'T return null
                    // This allows us to return homepage data while also queuing contact page
                    await context.enqueueRequest({
                        url: contactUrl,
                        userData: {
                            isContact: true,
                            originalUrl: request.userData?.originalUrl || website
                        }
                    });
                    log.info(`✅ Enqueued contact page: ${contactUrl} (homepage returned ${contactData.phones.length} phones)`);
                }
            }

            const pageType = isContact ? 'contact page' : 'homepage';
            log.info(`✅ Found ${contactData.phones.length} phone(s) on ${pageType}: ${website}`);

            return {
                website: request.userData?.originalUrl || website,
                contactUrl: isContact ? request.url : null,
                phones: contactData.phones.length ? contactData.phones : null,
                pageType: pageType,
                status: 'success'
            };

        } catch (err) {
            log.error(`❌ Error scraping ${website}: ${err.message}`);
            return {
                website: request.userData?.originalUrl || website,
                phones: null,
                pageType: 'unknown',
                status: 'error',
                error: err.message
            };
        }
    }
    """

    def run_website_scraper(client, websites):
        """Run Apify scraper for a batch of websites"""
        start_urls = [{"url": website, "userData": {"originalUrl": website}} for website in websites]

        print(f"  📋 Processing {len(start_urls)} websites in single actor run")

        run_input = {
            "startUrls": start_urls,
            "useChrome": False,              # Use Chromium (lighter, cheaper)
            "headless": True,
            "stealth": True,
            "pageFunction": create_website_scraper_pagefunction(),
            "maxRequestRetries": MAX_RETRIES,
            "maxRequestsPerCrawl": len(start_urls) * 2,  # Account for main + contact pages
            "maxConcurrency": MAX_CONCURRENCY,
            "pageLoadTimeoutSecs": PAGE_TIMEOUT,       # Reduced from 30s
            "pageFunctionTimeoutSecs": FUNCTION_TIMEOUT, # Reduced from 60s
            "waitUntil": ["domcontentloaded"],         # Fast page load strategy
            "ignoreSslErrors": True,                   # Skip SSL validation (faster)
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["SHADER"],
            },
        }

        print(f"  🚀 Launching Apify actor with {MAX_CONCURRENCY} concurrent browsers...")

        try:
            run = client.actor("apify/puppeteer-scraper").call(run_input=run_input)

            if not run or not isinstance(run, dict) or 'id' not in run:
                return [], f"API returned invalid response: {run}"

            print(f"  ⏳ Run ID: {run['id']}")

            run_client = client.run(run["id"])
            run_info = run_client.wait_for_finish()

            status = run_info.get('status', 'UNKNOWN')
            print(f"  📊 Status: {status}")

            if status in ['FAILED', 'TIMED-OUT', 'ABORTED']:
                error_detail = run_info.get('statusMessage', 'No error details')
                return [], f"Actor run {status}: {error_detail}"

            if status == "SUCCEEDED" and "defaultDatasetId" in run:
                dataset = client.dataset(run["defaultDatasetId"])
                items = list(dataset.iterate_items())
                print(f"  ✅ Retrieved {len(items)} results")
                return items, None

            return [], f"Scraping failed with status: {status}"

        except Exception as e:
            error_msg = f"Error during scraping: {type(e).__name__}: {str(e)}"
            print(f"  ❌ {error_msg}")
            return [], error_msg


    # Execute scraper
    print("="*70)
    print("🌐 WEBSITE PHONE NUMBER SCRAPER - COST-OPTIMIZED")
    print("="*70)
    print(f"📊 Configuration:")
    print(f"   • Batch size: {BATCH_SIZE} websites (increased for efficiency)")
    print(f"   • Concurrency: {MAX_CONCURRENCY} browsers (parallel processing)")
    print(f"   • Page timeout: {PAGE_TIMEOUT}s (reduced for speed)")
    print(f"   • Function timeout: {FUNCTION_TIMEOUT}s (optimized)")
    print(f"   • Retries: {MAX_RETRIES} (minimize compute waste)")
    print(f"   • Browser: Chromium (lightweight)")
    print(f"   • Proxy: DATACENTER (cost-optimized)")
    print(f"   • Strategy: Contact page → Homepage fallback")
    print("="*70)

    # Validate API token
    print(f"\n🔑 Validating Apify API token...")
    try:
        user_info = client.user().get()
        print(f"✅ API Key valid - User: {user_info.get('username', 'Unknown')}")
        print(f"   • Plan: {user_info.get('plan', {}).get('id', 'Unknown')}")
        print(f"   • Credits remaining: Check your dashboard at https://console.apify.com/billing")
    except Exception as e:
        print(f"❌ API Token Error: {e}")
        print(f"   • Check your token at: https://console.apify.com/account/integrations")
        raise

    # Use RecordOwl_Leads dataframe - filter for rows with valid Website column
    print(f"\n📋 Total rows in RecordOwl_Leads: {len(RecordOwl_Leads)}")

    # Filter for rows with non-null and non-empty websites
    websites_to_scrape = RecordOwl_Leads[
        RecordOwl_Leads["Website"].notna() &
        (RecordOwl_Leads["Website"] != "") &
        (RecordOwl_Leads["Website"] != "None")
    ].copy()

    print(f"📋 Rows with valid websites: {len(websites_to_scrape)}")

    # STEP 1: Verify website accessibility
    print(f"\n{'='*70}")
    print("🔍 STEP 1: VERIFYING WEBSITE ACCESSIBILITY")
    print(f"{'='*70}")
    print("Testing HTTP/HTTPS connectivity for all websites...")

    verified_count = 0
    failed_count = 0
    skipped_count = 0

    for idx, row in websites_to_scrape.iterrows():
        website = row['Website']

        # Check if website contains any skip keywords
        website_lower = website.lower()
        skip_keyword_found = None
        for keyword in SKIP_KEYWORDS:
            if keyword in website_lower:
                skip_keyword_found = keyword
                break

        if skip_keyword_found:
            # Skip this website
            websites_to_scrape.at[idx, 'Website_Verified'] = 'no'
            websites_to_scrape.at[idx, 'Verification_Info'] = f'Skipped - keyword blocked: {skip_keyword_found}'
            skipped_count += 1
            print(f"  ⏭️  {website} - Skipped (contains '{skip_keyword_found}')")
        else:
            # Verify the website
            verify_status, verify_info = verify_website_accessibility(website)

            websites_to_scrape.at[idx, 'Website_Verified'] = verify_status
            websites_to_scrape.at[idx, 'Verification_Info'] = verify_info

            if verify_status == 'yes':
                verified_count += 1
                print(f"  ✅ {website}")
            else:
                failed_count += 1
                print(f"  ❌ {website} - {verify_info}")

    print(f"\n📊 Verification Summary:")
    print(f"   • Verified (accessible): {verified_count}")
    print(f"   • Failed (inaccessible): {failed_count}")
    print(f"   • Skipped (blocked keywords): {skipped_count}")

    # Filter to only verified websites for scraping
    verified_websites = websites_to_scrape[
        websites_to_scrape['Website_Verified'] == 'yes'
    ].copy()

    print(f"\n✅ Proceeding to scrape {len(verified_websites)} verified websites")
    print(f"{'='*70}")

    # STEP 2: Scrape only verified websites
    print(f"\n{'='*70}")
    print("🌐 STEP 2: SCRAPING VERIFIED WEBSITES")
    print(f"{'='*70}")

    all_results = []
    total_rows = len(verified_websites)
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE

    if total_rows == 0:
        print("⚠️ No verified websites to scrape!")
    else:
        for batch_idx in range(0, total_rows, BATCH_SIZE):
            batch = verified_websites.iloc[batch_idx:batch_idx + BATCH_SIZE]
            websites = [str(row['Website']).strip() for _, row in batch.iterrows()]

            batch_num = (batch_idx//BATCH_SIZE)+1
            print(f"\n{'─'*70}")
            print(f"📦 Batch {batch_num}/{total_batches} - Processing {len(websites)} verified websites")

            items, error = run_website_scraper(client, websites)

            if error:
                print(f"  ❌ Batch error: {error}")
                for website in websites:
                    all_results.append({
                        "Website": website,
                        "Website_Scrape_Status": "error",
                        "Website_Scrape_Error": error,
                        "Website_Phones": None,
                        "Website_Contact_Page": None,
                        "Website_Page_Type": None
                    })
                continue

            # Map results by website
            website_map = {}
            for item in items:
                if item and item.get('website'):
                    # Store the result with most phones
                    web = item['website']
                    # Safely get phone counts, treating None as 0
                    current_phones = item.get('phones') or []
                    existing_phones = website_map[web].get('phones') or [] if web in website_map else []

                    # Store if new entry or has more phones than existing
                    if web not in website_map or len(current_phones) > len(existing_phones):
                        website_map[web] = item

            for website in websites:
                item = website_map.get(website)
                if not item:
                    print(f"    ⚠️  {website}: Not found in results")
                    all_results.append({
                        "Website": website,
                        "Website_Scrape_Status": "missing",
                        "Website_Scrape_Error": "No data returned",
                        "Website_Phones": None,
                        "Website_Contact_Page": None,
                        "Website_Page_Type": None
                    })
                else:
                    status = item.get('status', 'error')
                    phones = item.get('phones', None)
                    page_type = item.get('pageType', 'unknown')
                    phone_count = len(phones) if phones else 0

                    if status == 'success' and phones:
                        print(f"    ✅ {website}: {phone_count} phone(s) from {page_type}")
                    elif status == 'success':
                        print(f"    ⚠️  {website}: No phones found on {page_type}")
                    else:
                        print(f"    ❌ {website}: {status} - {item.get('error', 'Unknown')}")

                    all_results.append({
                        'Website': website,
                        'Website_Scrape_Status': status,
                        'Website_Scrape_Error': item.get('error'),
                        'Website_Phones': phones,
                        'Website_Contact_Page': item.get('contactUrl'),
                        'Website_Page_Type': page_type
                    })

            # Sleep between batches
            if batch_num < total_batches:
                time.sleep(2)

    # Create results DataFrame from scraping results
    Website_Scraped_Results = pd.DataFrame(all_results)

    # Merge scraping results with original RecordOwl_Leads (no verification columns)
    RecordOwl_Leads_Enriched = RecordOwl_Leads.merge(
        Website_Scraped_Results,
        on='Website',
        how='left'
    )

    print(f"\n{'='*70}")
    print("✅ PROCESSING COMPLETE")
    print(f"{'='*70}")
    print(f"📊 Final Statistics:")
    print(f"   • Total rows: {len(RecordOwl_Leads_Enriched)}")
    print(f"   • Websites verified accessible: {verified_count}")
    print(f"   • Websites skipped (keywords): {skipped_count}")
    print(f"   • Websites scraped: {len(Website_Scraped_Results)}")
    print(f"   • Phones found: {len(RecordOwl_Leads_Enriched[RecordOwl_Leads_Enriched['Website_Phones'].notna()])}")
    print(f"{'='*70}\n")

    return RecordOwl_Leads_Enriched
