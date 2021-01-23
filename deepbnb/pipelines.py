# -*- coding: utf-8 -*-
import elasticsearch.exceptions
import re
import webbrowser

from deepbnb.model import Listing
from scrapy.exceptions import DropItem


class BnbPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            minimum_monthly_discount=crawler.settings.get('MINIMUM_MONTHLY_DISCOUNT'),
            minimum_weekly_discount=crawler.settings.get('MINIMUM_WEEKLY_DISCOUNT'),
            minimum_photos=crawler.settings.get('MINIMUM_PHOTOS'),
            skip_list=crawler.settings.get('SKIP_LIST'),
            cannot_have=crawler.settings.get('CANNOT_HAVE'),
            must_have=crawler.settings.get('MUST_HAVE'),
            property_type_blacklist=crawler.settings.get('PROPERTY_TYPE_BLACKLIST'),
            feed_format=crawler.settings.get('FEED_FORMAT'),  # output file type, autogenerated from -o file ext.
            web_browser=crawler.settings.get('WEB_BROWSER')
        )

    def __init__(
            self,
            minimum_monthly_discount,
            minimum_weekly_discount,
            minimum_photos,
            skip_list,
            cannot_have,
            must_have,
            property_type_blacklist,
            feed_format,
            web_browser
    ):
        """Class constructor."""
        self._feed_format = feed_format
        # self._fields_to_check = ['description', 'name', 'summary', 'notes']
        self._fields_to_check = ['description', 'name']
        self._minimum_monthly_discount = minimum_monthly_discount
        self._minimum_weekly_discount = minimum_weekly_discount
        self._minimum_photos = minimum_photos

        self._skip_list = skip_list
        self._property_type_blacklist = property_type_blacklist

        self._cannot_have_regex = cannot_have
        if self._cannot_have_regex:
            self._cannot_have_regex = re.compile(str(self._cannot_have_regex), re.IGNORECASE)

        self._must_have_regex = must_have
        if self._must_have_regex:
            self._must_have_regex = re.compile(str(self._must_have_regex), re.IGNORECASE)

        self._web_browser = web_browser
        if self._web_browser:
            self._web_browser = webbrowser.get(web_browser + ' %s')  # append URL placeholder (%s)

    def process_item(self, item, spider):
        """Drop items not fitting parameters. Open in browser if specified. Return accepted items."""

        if self._skip_list and str(item.get('id')) in self._skip_list:
            raise DropItem('Item in skip list: {}'.format(item['id']))

        if self._property_type_blacklist and item['room_and_property_type'] in self._property_type_blacklist:
            raise DropItem('Skipping property type: {}'.format(item['room_and_property_type']))

        if self._minimum_monthly_discount and 'monthly_discount' in item:
            if item['monthly_discount'] < self._minimum_monthly_discount:
                raise DropItem('Monthly discount too low: {}'.format(item['monthly_discount']))

        if self._minimum_weekly_discount and 'weekly_discount' in item:
            if item['weekly_discount'] < self._minimum_monthly_discount:
                raise DropItem('Weekly discount too low: {}'.format(item['weekly_discount']))

        if self._minimum_photos and item['photo_count'] < self._minimum_photos:
            raise DropItem('Photos too low: {} photos'.format(item['photo_count']))

        # check regexes
        if self._cannot_have_regex:
            for f in self._fields_to_check:
                field_val = item[f]
                if field_val is None:
                    continue
                v = str(field_val.encode('ASCII', 'replace'))
                if self._cannot_have_regex.search(v):
                    raise DropItem('Found: {}'.format(self._cannot_have_regex.pattern))

        if self._must_have_regex:
            has_must_haves = False
            for f in self._fields_to_check:
                field_val = item[f]
                if field_val is None:
                    continue
                v = str(field_val.encode('ASCII', 'replace'))
                if self._must_have_regex.search(v):
                    has_must_haves = True
                    break

            if not has_must_haves:
                raise DropItem('Not Found: {}'.format(self._must_have_regex.pattern))

        if self._web_browser:  # open in browser
            self._web_browser.open_new_tab(item['url'])

        return item

fileIndex = 0;
class ElasticBnbPipeline:
    def process_item(self, item, spider):
        """Insert / update items in ElasticSearch."""
        properties = {
            'access':                 item['access'],
            'additional_house_rules': item['additional_house_rules'],
            'allows_events':          item['allows_events'],
            'amenities':              item['amenities'],
            'amenity_ids':            item['amenity_ids'],
            'avg_rating':             item['avg_rating'],
            'bathrooms':              item['bathrooms'],
            'bedrooms':               item['bedrooms'],
            'beds':                   item['beds'],
            'business_travel_ready':  item['business_travel_ready'],
            'city':                   item['city'],
            'country':                item['country'],
            'coordinates':            {'lon': item['longitude'], 'lat': item['latitude']},
            'description':            item['description'],
            'host_id':                item['host_id'],
            'house_rules':            item['house_rules'],
            'interaction':            item.get('interaction'),
            'is_hotel':               item['is_hotel'],
            'max_nights':             item['max_nights'],
            'min_nights':             item['min_nights'],
            'monthly_price_factor':   item['monthly_price_factor'],
            'name':                   item['name'],
            'neighborhood_overview':  item['neighborhood_overview'],
            # 'notes':                  item['notes'],
            'person_capacity':        item['person_capacity'],
            'photo_count':            item['photo_count'],
            'place_id':               item['place_id'],
            'price_rate':             item['price_rate'],
            'price_rate_type':        item['price_rate_type'],
            'province':               item['province'],
            'rating_accuracy':        item['rating_accuracy'],
            'rating_checkin':         item['rating_checkin'],
            'rating_cleanliness':     item['rating_cleanliness'],
            'rating_communication':   item['rating_communication'],
            'rating_location':        item['rating_location'],
            'rating_value':           item['rating_value'],
            'review_count':           item['review_count'],
            'review_score':           item['review_score'],
            'room_and_property_type': item['room_and_property_type'],
            'room_type':              item['room_type'],
            'room_type_category':     item['room_type_category'],
            'satisfaction_guest':     item['satisfaction_guest'],
            'star_rating':            item['star_rating'],
            'state':                  item['state'],
            'transit':                item['transit'],
            'url':                    item['url'],
            'weekly_price_factor':    item['weekly_price_factor']
            
            
            #with open("testFile" + str(fileIndex) + ".txt", "w+") as f:
                #f.write(str(item));
                #f.close();
                
           #fileIndex = fileIndex + 1;
        }

        try:
            listing = Listing.get(id=item['id'])
            listing.update(**properties)
        except elasticsearch.exceptions.NotFoundError:
            properties['meta'] = {'id': item['id']}
            listing = Listing(**properties)
            listing.save()

        return item


class DuplicatesPipeline:
    """Looks for duplicate items, and drops those items that were already processed

    @ref: https://docs.scrapy.org/en/latest/topics/item-pipeline.html#duplicates-filter
    """

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['id'])
            return item
