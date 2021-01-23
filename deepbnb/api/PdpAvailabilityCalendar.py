import json;
import requests;
import scrapy;

from urllib.parse import urlparse, parse_qs;

from deepbnb.api.ApiBase import ApiBase;

from datetime import datetime;

# Metallicafan212: Calendar info for listings
class PdpAvailabilityCalendar(ApiBase):
    """Airbnb API v3 Calendar Endpoint"""

    def api_request(self, listing_id: str, limit: int = 7, start_offset: int = 0):
        """Perform API request."""
        # Metallicafan212: Get the calendar response
        calendar = self._get_calendar(listing_id, limit, start_offset);

        # get any additional batches
        #start_idx = start_offset + limit
        #for offset in range(start_idx, n_days_total, limit):
        #    r, _ = self._get_calendar_batch(listing_id, limit, offset);
        #    calendar.extend(r);

        return calendar;

    def _get_calendar(self, listing_id: str, limit: int, offset: int):
        # Metallicafan212: Get all the calendar info for a given listing ID
        url             = self._get_url(listing_id, limit, offset);
        headers         = self._get_search_headers();
        response        = requests.get(url, headers=headers);
        
        with open("CalendarDebug.txt", "w+") as f:
            f.write(response.text);
            f.close();
        
        data            = json.loads(response.text);
        pdp_calendar    = data['data']['merlin']['pdpAvailabilityCalendar'];
        #n_reviews_total = int(pdp_reviews['metadata']['reviewsCount'])
        
        
        
        # Metallicafan212: Process the calendar months
        #                  The days and months can be assumed from their order, so I will not add in the labels they have
        calendar = [{
            'month'         : m['month'],
            'year'          : m['year'],
            'days'          : [{
                'available'             : d['available'],
                'maxNights'             : d['maxNights'],
                'minNights'             : d['minNights'],
                'availableForCheckin'   : d['availableForCheckin'],
                'availableForCheckout'  : d['availableForCheckout'],
                'bookable'              : d['bookable'],
                'price'                 : d['price']['localPriceFormatted'],
            } for d in m['days']],
            'conditionRanges' : [{
                'startDate'             : c['startDate'],
                'endDate'               : c['endDate'],
                'conditions'            : [{
                    'closedToArrival'   : c['conditions']['closedToArrival'],
                    'closedToDeparture' : c['conditions']['closedToDeparture'],
                    'endDayOfWeek'      : c['conditions']['endDayOfWeek'],
                    'maxNights'         : c['conditions']['maxNights'],
                    'minNights'         : c['conditions']['minNights'],
               }]
            } for c in m['conditionRanges']],
        } for m in pdp_calendar['calendarMonths']];
                
        
        '''
        reviews = [{
            'comments':   r['comments'],
            'created_at': r['createdAt'],
            'language':   r['language'],
            'rating':     r['rating'],
            'response':   r['response'],
        } for r in pdp_reviews['reviews']]
        '''

        return calendar;#, n_reviews_total

    def _get_url(self, listing_id: str, limit: int = 7, offset: int = None) -> str:
    #https://www.airbnb.com/api/v3/PdpAvailabilityCalendar?operationName=PdpAvailabilityCalendar&locale=en&currency=USD&variables=%7B%22request%22%3A%7B%22count%22%3A12%2C%22listingId%22%3A%2222978260%22%2C%22month%22%3A1%2C%22year%22%3A2021%7D%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22b94ab2c7e743e30b3d0bc92981a55fff22a05b20bcc9bcc25ca075cc95b42aac%22%7D%7D&_cb=135me1o1ijh3jq
        _api_path = '/api/v3/PdpAvailabilityCalendar';#'/api/v3/PdpReviews'
        
        # Metallicafan212: We need today's date
        today = datetime.today();
        
        # Metallicafan212: Todo! Figure out all the variables!
        query = {
            'operationName'     : 'PdpAvailabilityCalendar',
            'locale'            : 'en',
            'currency'          : self._currency,
            'variables'         : {
                'request'       : {
                    'count'     :       12,
                    'listingId' :       listing_id,
                    'month'     :       today.month,
                    'year'      :       today.year,
                }
            },
            'extensions'        : {
                'persistedQuery': {
                    'version'   :    1,
                    'sha256Hash': 'b94ab2c7e743e30b3d0bc92981a55fff22a05b20bcc9bcc25ca075cc95b42aac'#'4730a25512c4955aa741389d8df80ff1e57e516c469d2b91952636baf6eee3bd'
                }
            }
        }

        #if offset:
        #    query['variables']['request']['offset'] = offset

        self._put_json_param_strings(query);

        return self._build_airbnb_url(_api_path, query);

    def _parse_calendar(self, response):
        # parse qs
        parsed = urlparse(response.request.url);
        parsed_qs = parse_qs(parsed.query);
        variables = json.loads(parsed_qs['variables'][0]);

        # extract data
        listing_id      = variables['request']['listingId'];
        data            = self.read_data(response);
        print(data);
        pdp_calendar    = data['data']['merlin']['pdpAvailabilityCalendar'];

        #if offset == 0:  # get all other reviews
            #for offset in range(limit, n_reviews_total, limit):
        url = self._get_url(listing_id, limit, offset);
        yield scrapy.Request(url, callback=self._parse_calendar, headers=self._get_search_headers());

        # Metallicafan212: Return distilled Calender
        yield from ({
            'month'         : m['month'],
            'year'          : m['year'],
            'days'          : [{
                'available'             : d['available'],
                'maxNights'             : d['maxNights'],
                'minNights'             : d['minNights'],
                'availableForCheckin'   : d['availableForCheckin'],
                'availableForCheckout'  : d['availableForCheckout'],
                'bookable'              : d['bookable'],
                'price'                 : d['price']['localPriceFormatted'],
            } for d in m['days']],
            'conditionRanges' : [{
                'startDate'             : c['startDate'],
                'endDate'               : c['endDate'],
                'conditions'            : [{
                    'closedToArrival'   : c['conditions']['closedToArrival'],
                    'closedToDeparture' : c['conditions']['closedToDeparture'],
                    'endDayOfWeek'      : c['conditions']['endDayOfWeek'],
                    'maxNights'         : c['conditions']['maxNights'],
                    'minNights'         : c['conditions']['minNights'],
               }]
            } for c in m['conditionRanges']],
        } for m in pdp_calendar['calendarMonths']);