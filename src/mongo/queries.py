# src/mongo/queries.py

import logging
from typing import List, Dict, Any
from datetime import datetime
from pymongo import ASCENDING, DESCENDING

logger = logging.getLogger(__name__)

class MongoQueries:
    """Clase para gestionar consultas MongoDB específicas"""
    
    def __init__(self, db):
        self.db = db
    
    def get_business_stats_by_city(self, limit: int = 10) -> List[Dict]:
        """Obtener estadísticas de negocios por ciudad"""
        pipeline = [
            {
                "$group": {
                    "_id": "$city",
                    "avg_stars": {"$avg": "$stars"},
                    "business_count": {"$sum": 1},
                    "total_reviews": {"$sum": "$review_count"},
                    "avg_reviews": {"$avg": "$review_count"}
                }
            },
            {
                "$match": {
                    "_id": {"$ne": None},
                    "business_count": {"$gt": 10}
                }
            },
            {
                "$sort": {"avg_stars": -1}
            },
            {
                "$limit": limit
            }
        ]
        
        return list(self.db.business.aggregate(pipeline))
    
    def get_top_businesses(self, min_reviews: int = 50, limit: int = 5) -> List[Dict]:
        """Obtener los mejores negocios con mínimo de reseñas"""
        pipeline = [
            {
                "$match": {
                    "review_count": {"$gte": min_reviews},
                    "stars": {"$gte": 4.0}
                }
            },
            {
                "$sort": {"stars": -1, "review_count": -1}
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "city": 1,
                    "state": 1,
                    "stars": 1,
                    "review_count": 1,
                    "categories": 1
                }
            }
        ]
        
        return list(self.db.business.aggregate(pipeline))
    
    def get_most_active_users(self, limit: int = 10) -> List[Dict]:
        """Obtener usuarios más activos"""
        pipeline = [
            {
                "$match": {
                    "review_count": {"$gt": 0}
                }
            },
            {
                "$sort": {"review_count": -1}
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "user_id": 1,
                    "name": 1,
                    "review_count": 1,
                    "yelping_since": 1,
                    "useful": 1,
                    "funny": 1,
                    "cool": 1
                }
            }
        ]
        
        return list(self.db.user.aggregate(pipeline))
    
    def get_reviews_by_date_range(self, start_date: str, end_date: str, limit: int = 20) -> List[Dict]:
        """Obtener reseñas por rango de fechas"""
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": datetime.fromisoformat(start_date),
                        "$lte": datetime.fromisoformat(end_date)
                    }
                }
            },
            {
                "$sort": {"date": -1}
            },
            {
                "$limit": limit
            },
            {
                "$lookup": {
                    "from": "business",
                    "localField": "business_id",
                    "foreignField": "business_id",
                    "as": "business_info"
                }
            },
            {
                "$lookup": {
                    "from": "user",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$business_info"
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$project": {
                    "_id": 0,
                    "review_id": 1,
                    "stars": 1,
                    "text": {"$substr": ["$text", 0, 100]},
                    "date": 1,
                    "business_name": "$business_info.name",
                    "business_city": "$business_info.city",
                    "user_name": "$user_info.name"
                }
            }
        ]
        
        return list(self.db.review.aggregate(pipeline))
    
    def get_business_categories_analysis(self) -> List[Dict]:
        """Analizar categorías de negocios"""
        pipeline = [
            {
                "$match": {
                    "categories": {"$exists": True, "$ne": None}
                }
            },
            {
                "$unwind": "$categories"
            },
            {
                "$group": {
                    "_id": "$categories",
                    "count": {"$sum": 1},
                    "avg_stars": {"$avg": "$stars"},
                    "avg_reviews": {"$avg": "$review_count"}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 20
            }
        ]
        
        return list(self.db.business.aggregate(pipeline))
    
    def get_user_review_patterns(self, user_id: str) -> Dict:
        """Obtener patrones de reseñas de un usuario específico"""
        pipeline = [
            {
                "$match": {"user_id": user_id}
            },
            {
                "$lookup": {
                    "from": "business",
                    "localField": "business_id",
                    "foreignField": "business_id",
                    "as": "business_info"
                }
            },
            {
                "$unwind": "$business_info"
            },
            {
                "$group": {
                    "_id": None,
                    "total_reviews": {"$sum": 1},
                    "avg_rating": {"$avg": "$stars"},
                    "favorite_categories": {
                        "$addToSet": "$business_info.categories"
                    },
                    "cities_visited": {
                        "$addToSet": "$business_info.city"
                    },
                    "rating_distribution": {
                        "$push": "$stars"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_reviews": 1,
                    "avg_rating": 1,
                    "favorite_categories": {
                        "$slice": [{"$setUnion": "$favorite_categories"}, 10]
                    },
                    "cities_visited": 1,
                    "rating_stats": {
                        "min": {"$min": "$rating_distribution"},
                        "max": {"$max": "$rating_distribution"}
                    }
                }
            }
        ]
        
        result = list(self.db.review.aggregate(pipeline))
        return result[0] if result else {}