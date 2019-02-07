import logging
from datamodel.search.Yunfeiz1Puc1_datamodel import Yunfeiz1Puc1Link, OneYunfeiz1Puc1UnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
from urlparse import urlparse, parse_qs
from uuid import uuid4
# import BeautifulSoup4 and urllib2 for url parsing
from bs4 import BeautifulSoup
import urllib2

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
# Keep track of how many urls have been processed for each different subdomains
subdomaincount = dict()
# The page with the most out links
max_url = "http://www.ics.uci.edu/"
# The max number of outlinks of pages processed
max_outlink = 0
# How many urls have been downloaded
url_count = 0
# Limitation of number of urls to download
max_links = 3000

@Producer(Yunfeiz1Puc1Link)
@GetterSetter(OneYunfeiz1Puc1UnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Yunfeiz1Puc1"

    def __init__(self, frame):
        self.app_id = "Yunfeiz1Puc1"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneYunfeiz1Puc1UnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = Yunfeiz1Puc1Link("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneYunfeiz1Puc1UnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            # Stop the crawler when it has crawled max_links pages
            global url_count
            if url_count > max_links:
                save_to_file()
                self.shutdown()

            print "Got a link to download:", link.full_url
            downloaded = link.download()
            url_count += 1
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(Yunfeiz1Puc1Link(l))

    def shutdown(self):
        print ("Nice crawling!")
        exit()

def extract_next_links(rawDataObj):
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    outputLinks = []
    # Check for error_message
    if rawDataObj.error_message:
        return outputLinks

    if rawDataObj.final_url:
        current_url = rawDataObj.final_url
    else:
        current_url = rawDataObj.url

    outputLinks = extract_links_from_html(rawDataObj.content, current_url)
    return outputLinks

def extract_links_from_html(html, current_url):
    '''
    Takes in text in html format and return a list of urls found
    in the text using the BeautifulSoup library and lxml parser.
    Skip pags that contains urls leading to themselves.
    '''

    links = []
    # loads the html into a BeautifulSoup object
    soup = BeautifulSoup(html, "lxml")
    # Only find links starting with https://
    for link in soup.findAll('a', attrs={'href': re.compile("^https://")}):
        if is_same_url(link, current_url):
            continue
        links.append(link.get('href'))
    # Only find links starting with http://
    for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
        if is_same_url(link, current_url):
            continue
        links.append(link.get('href'))
    # Keep track of crawer activity
    analytics(current_url, links)

    return links

def is_same_url(link, current_url):
    '''
    Check if two urls point to the same webpage
    '''

    if link == current_url:
        return True
    if str(link) + '/' == str(current_url) or str(link) == str(current_url) + '/':
        return True

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''

    parsed = urlparse(url)

    if re.search("(\d{4})[/-](\d{2})", parsed.path):
        return False

    if "calendar" in parsed.path:
        return False

    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|csv|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False

def analytics(url, links):
    '''
    Count how many urls for each subdomains have been processed
    and find the page with the most outlinks
    '''

    parsed = urlparse(url)
    # Only count subdomains of ics.uci.edu
    if parsed.hostname and ".ics.uci.edu" in parsed.hostname:
        subdomain = parsed.hostname.split('.')[0]
        global subdomaincount
        if subdomain != "www":
            subdomaincount[subdomain] = subdomaincount.get(subdomain, 0) + 1


    current_outlink = len(links)
    global max_outlink
    global max_url
    if max_outlink < current_outlink:
        max_url = url
        max_outlink = current_outlink

def save_to_file():
    '''
    Save analytics of the crawler to a text file with information of
    how many urls have been processed from each subdomains and the
    page with the most outputLinks
    '''

    with open('analytics.txt', 'w') as file:
        #for key, value in subdomaincount.iteritems():
        for key, value in sorted(subdomaincount.iteritems(), key=lambda (k,v): (v,k), reverse = True):
            file.write("{}: {}\n".format(key, value))
        file.write("MOST OUT LINKS: " + max_url + "\nOUT LINKS: " + str(max_outlink) + "\n")
