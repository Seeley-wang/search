import json

import datetime

import redis as redis
from django.shortcuts import render
from django.views.generic.base import View
from search.models import ArticleType, ZhiHuQuestionType, LagouType
from django.http import HttpResponse
from elasticsearch import Elasticsearch

# # Create your views here.
client = Elasticsearch(hosts=['127.0.0.1'])
redis_cli = redis.StrictRedis()


class IndexView(View):
    def get(self, request):
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get('s', '')
        current_type = request.GET.get('s_type', '')

        if current_type == 'article':
            re_datas = []
            if key_words:
                s = ArticleType.search()
                s = s.suggest('my_suggest', key_words, completion={
                    'field': 'suggest', 'fuzzy': {
                        "fuzziness": 3
                    },
                    'size': 10
                })
                suggestions = s.execute().suggest
                for match in suggestions.my_suggest[0].options:
                    source = match._source
                    re_datas.append(source['title'])

            return HttpResponse(json.dumps(re_datas), content_type='application/json')
        elif current_type == 'question':
            re_datas = []
            if key_words:
                s = ZhiHuQuestionType.search()
                s = s.suggest('my_suggest', key_words, completion={
                    'field': 'suggest', 'fuzzy': {
                        "fuzziness": 3
                    },
                    'size': 10
                })

                suggestions = s.execute().suggest
                for match in suggestions[0].options:
                    source = match._source
                    re_datas.append(source['title'])
            return HttpResponse(json.dumps(re_datas), content_type='application/json')
        elif current_type == 'job':
            re_datas = []
            if key_words:
                s = LagouType().search()
                s = s.suggest('my_suggest', key_words, completion={
                    'field': 'suggest', 'fuzzy': {
                        "fuzziness": 3
                    },
                    'size': 10
                })
                suggestions = s.execute().suggest
                for match in suggestions[0].options:
                    source = match._source
                    re_datas.append(source['title'])
            return HttpResponse(json.dumps(re_datas), content_type='application/json')


class SearchView(View):
    def get(self, request):
        topn_search_clean = []
        key_words = request.GET.get('q', '')

        redis_cli.zincrby("search_keywords_set", key_words)

        topn_search = redis_cli.zrevrangebyscore('search_keywords_set', '+inf', '-inf', start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        page = request.GET.get('p', '')
        try:
            page = int(page)
        except:
            page = 1
        jobbole_count = redis_cli.get('jobbole_count')
        start_time = datetime.datetime.now()

        s_type = request.GET.get('s_type', '')
        if s_type == 'article':
            response = client.search(
                index='jobbole',
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["title", "tags", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ["<span class='keyWord'>"],
                        "post_tags": ["</span>"],
                        "fields": {
                            "title": {},
                            "content": {}
                        }
                    }
                }
            )
        elif s_type == 'job':
            response = client.search(
                index='lagou',
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["title", "tags", "job_desc", "job_addr", "company_name"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ["<span class='keyWord'>"],
                        "post_tags": ["</span>"],
                        "fields": {
                            "title": {},
                            "job_desc": {},
                            "job_addr": {},
                            "company_name": {}
                        }
                    }
                }
            )
        elif s_type == 'question':
            response = client.search(
                index='zhihu',
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["topics", "title", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ["<span class='keyWord'>"],
                        "post_tags": ["</span>"],
                        "fields": {
                            "title": {},
                            "content": {}
                        }
                    }
                }
            )

        end_time = datetime.datetime.now()
        last_seconds = (end_time - start_time).total_seconds()

        total_nums = response['hits']['total']
        if (page % 10) > 0:
            page_nums = int(total_nums / 10 + 1)
        else:
            page_nums = int(total_nums / 10)

        if s_type == 'article':
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if 'title' in hit['highlight']:
                    hit_dict['title'] = ''.join(hit['highlight']['title'])
                else:
                    hit_dict['title'] = ''.join(hit['_source']['title'])
                if 'content' in hit['highlight']:
                    hit_dict['content'] = ''.join(hit['highlight']['content'][:500])
                else:
                    hit_dict['content'] = ''.join(hit['_source']['content'][:500])

                hit_dict['create_date'] = hit['_source']['create_date']
                hit_dict['url'] = hit['_source']['url']
                hit_dict['score'] = hit['_score']
                hit_dict["source_site"] = "伯乐在线"
                hit_list.append(hit_dict)
        elif s_type == 'job':
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if 'title' in hit['highlight']:
                        hit_dict['title'] = ''.join(hit['highlight']['title'])
                    else:
                        hit_dict['title'] = ''.join(hit['_source']['title'])
                    if 'job_desc' in hit['highlight']:
                        hit_dict['content'] = ''.join(hit['highlight']['job_desc'][:500])
                    else:
                        hit_dict['content'] = ''.join(hit['_source']['job_desc'][:500])
                except:
                    hit_dict['title'] = ''.join(hit['_source']['title'])
                    hit_dict['content'] = ''.join(hit['_source']['job_desc'][:500])
                    hit_dict['company_name'] = ''.join(hit['_source']['company_name'])
                    hit_dict['job_addr'] = ''.join(hit['_source']['job_addr'])

                hit_dict['create_date'] = hit['_source']['publish_time']
                hit_dict['url'] = hit['_source']['url']
                hit_dict['score'] = hit['_score']
                hit_dict["source_site"] = "拉勾网"
                hit_list.append(hit_dict)
        elif s_type == 'question':
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if hit["_type"] == 'answer':
                        if 'title' in hit['highlight']:
                            hit_dict['title'] = ''.join(hit['highlight']['title'])
                        else:
                            hit_dict['title'] = ''.join(hit['_source']['title'])
                        if 'content' in hit['highlight']:
                            hit_dict['content'] = ''.join(hit['highlight']['content'][:500])
                        else:
                            hit_dict['content'] = ''.join(hit['_source']['content'][:500])
                        hit_dict['create_date'] = hit['_source']['create_time']
                        question_id = hit['_source']['question_id']
                        answer_id = hit['_source']['answer_id']
                        hit_dict["url"] = "https://www.zhihu.com/question/{0}/answer/{1}".format(
                            question_id, answer_id)
                        hit_dict["source_site"] = "知乎"
                        hit_dict["score"] = hit["_score"]

                        hit_list.append(hit_dict)
                    elif hit["_type"] == "question":
                        if "title" in hit["highlight"]:
                            hit_dict["title"] = "".join(hit["highlight"]["title"])
                        else:
                            hit_dict["title"] = hit["_source"]["title"]
                        if "content" in hit["highlight"]:
                            hit_dict["content"] = "".join(hit["highlight"]["content"])
                        else:
                            hit_dict["content"] = hit["_source"]["content"]
                        hit_dict["create_date"] = datetime.datetime.now
                        hit_dict["url"] = hit["_source"]["url"]
                        hit_dict["score"] = hit["_score"]
                        hit_dict["source_site"] = "知乎"
                        hit_list.append(hit_dict)
                except:
                    if hit["_type"] == 'answer':
                        hit_dict['title'] = ''.join(hit['_source']['title'])
                        hit_dict['content'] = ''.join(hit['_source']['content'][:500])
                        hit_dict['create_date'] = hit['_source']['create_time']
                        question_id = hit['_source']['question_id']
                        answer_id = hit['_source']['answer_id']
                        hit_dict["url"] = "https://www.zhihu.com/question/{0}/answer/{1}".format(
                            question_id, answer_id)
                        hit_dict["score"] = hit["_score"]
                        hit_dict["source_site"] = "知乎"

                        hit_list.append(hit_dict)
                    elif hit["_type"] == "question":
                        hit_dict["title"] = hit["_source"]["title"]
                        hit_dict["content"] = hit["_source"]["content"]
                        hit_dict["create_date"] = datetime.datetime.now
                        hit_dict["url"] = hit["_source"]["url"]
                        hit_dict["score"] = hit["_score"]
                        hit_dict["source_site"] = "知乎"
                        hit_list.append(hit_dict)

        return render(request, 'result.html', {
            'all_hits': hit_list,
            'key_words': key_words,
            'total_nums': total_nums,
            'page': page,
            'page_nums': page_nums,
            'last_seconds': last_seconds,
            'jobbole_count': jobbole_count,
            'topn_search': topn_search,
            's_type': s_type,
        }
                      )
