from .config import *

def log_query(query, start, end, index = "vltlog*"):
    """ES query to retrieve logs

    Args:
        query (string): what are we looking for
        start (string, datetime): where to start the search
        end (string, datetime): where to end the search
        index (string): ES index where to look at.

    Returns:
        (pandas.DataFrame): hits of the query

    """
    client = Elasticsearch("wgsdlab5")
    s = Search(using = client, index=index).query({
        "bool": {
          "must": [
            {
              "query_string": {
                "query": query,
                "analyze_wildcard": "true",
                "default_field": "*"
              }
            },
            {
              "range": {
                "@timestamp": {
                  "gte": start,
                  "lte": end,
                }
              }
            }
          ]
        }
      }
    )
    
    response = s[0:s.count()].execute()
    df = pd.DataFrame((d.to_dict() for d in response.hits))
    return df

def log_scan(query, start, end, index = "vltlog*"):
    """ES scan to retrieve logs

    Args:
        query (string): what are we looking for
        start (string, datetime): where to start the search
        end (string, datetime): where to end the search
        index (string): ES index where to look at.

    Returns:
        (pandas.DataFrame): hits of the query

    """
    es = Elasticsearch("wgsdlab5")
    scanned = scan(es,
        query = {
            "query":
            {
                "bool": {
                  "must": [
                    {
                      "query_string": {
                        "query": query,
                        "analyze_wildcard": "true",
                        "default_field": "*"
                      }
                    },
                    {
                      "range": {
                        "@timestamp": {
                          "gte": start,
                          "lte": end,
                        }
                      }
                    }
                  ]
                }
            }
        },
        scroll='20m',
        size=1000,
        index = index,
        request_timeout = 100
    )
    
    res = pd.DataFrame([hit['_source'] for hit in scanned])
    return res
