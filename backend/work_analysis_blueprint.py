"""
Flask API endpoint integration example for work_while_studying analysis.

Add these endpoints to your Flask app (backend/app.py) or create a new blueprint.
"""

from flask import Blueprint, jsonify, request
from work_while_studying import computeWorkWhileStudying, ensure_work_while_studying_schema
from database import get_connection
import logging

logger = logging.getLogger(__name__)

# Create a blueprint for work-while-studying APIs
work_analysis_bp = Blueprint('work_analysis', __name__, url_prefix='/api/work-analysis')


# ============================================================
# SINGLE USER ANALYSIS
# ============================================================

@work_analysis_bp.route('/user/<int:user_id>', methods=['GET'])
def analyze_user_work_while_studying(user_id):
    """
    Analyze if a user was working while studying.
    
    Response:
    {
        "user_id": int,
        "graduation_year": int or null,
        "graduation_date_used": "YYYY-MM-DD" or null,
        "graduated_status": "graduated" | "not_yet_graduated" | "unknown",
        "is_working_while_studying": boolean,
        "evidence_jobs": [
            {
                "company": string,
                "title": string,
                "start_date": "YYYY-MM-DD" or null,
                "end_date": "YYYY-MM-DD" or null,
                "status": "worked_while_studying" | "worked_after_graduation"
            }
        ]
    }
    """
    try:
        result = computeWorkWhileStudying(user_id, get_connection)
        
        if result is None:
            return jsonify({
                "error": f"Failed to analyze user {user_id}",
                "user_id": user_id
            }), 500
        
        # Convert dates to ISO format strings for JSON
        result['graduation_date_used'] = (
            result['graduation_date_used'].isoformat() 
            if result['graduation_date_used'] 
            else None
        )
        
        for job in result['evidence_jobs']:
            job['start_date'] = job['start_date'].isoformat() if job['start_date'] else None
            job['end_date'] = job['end_date'].isoformat() if job['end_date'] else None
        
        return jsonify(result), 200
    
    except Exception as e:
        logger.error(f"Error analyzing user {user_id}: {e}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500


# ============================================================
# BATCH ANALYSIS
# ============================================================

@work_analysis_bp.route('/batch', methods=['POST'])
def analyze_batch_users():
    """
    Analyze multiple users for work-while-studying status.
    
    Request body:
    {
        "user_ids": [123, 456, 789]
    }
    
    Response:
    {
        "results": [
            { ...user analysis... },
            { ...user analysis... }
        ],
        "successful": 3,
        "failed": 0,
        "errors": []
    }
    """
    try:
        data = request.get_json()
        user_ids = data.get('user_ids', [])
        
        if not user_ids or not isinstance(user_ids, list):
            return jsonify({
                "error": "Invalid request",
                "message": "user_ids must be a non-empty list"
            }), 400
        
        if len(user_ids) > 1000:
            return jsonify({
                "error": "Too many users",
                "message": "Maximum 1000 users per batch"
            }), 400
        
        results = []
        errors = []
        
        for user_id in user_ids:
            try:
                result = computeWorkWhileStudying(user_id, get_connection)
                
                if result is None:
                    errors.append({
                        "user_id": user_id,
                        "error": "Failed to analyze"
                    })
                else:
                    # Convert dates to ISO format
                    result['graduation_date_used'] = (
                        result['graduation_date_used'].isoformat() 
                        if result['graduation_date_used'] 
                        else None
                    )
                    
                    for job in result['evidence_jobs']:
                        job['start_date'] = job['start_date'].isoformat() if job['start_date'] else None
                        job['end_date'] = job['end_date'].isoformat() if job['end_date'] else None
                    
                    results.append(result)
            
            except Exception as e:
                logger.warning(f"Error analyzing user {user_id}: {e}")
                errors.append({
                    "user_id": user_id,
                    "error": str(e)
                })
        
        return jsonify({
            "results": results,
            "successful": len(results),
            "failed": len(errors),
            "errors": errors if errors else None
        }), 200
    
    except Exception as e:
        logger.error(f"Error in batch analysis: {e}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500


# ============================================================
# STATISTICS & AGGREGATION
# ============================================================

@work_analysis_bp.route('/stats', methods=['GET'])
def get_work_while_studying_stats():
    """
    Get aggregate statistics about users working while studying.
    
    Response:
    {
        "total_analyzed": int,
        "working_while_studying_count": int,
        "graduated_count": int,
        "not_yet_graduated_count": int,
        "unknown_status_count": int,
        "percentage_working_while_studying": float
    }
    """
    conn = None
    try:
        conn = get_connection()
        
        with conn.cursor(dictionary=True) as cur:
            # Count users analyzed
            cur.execute("SELECT COUNT(*) as count FROM education")
            total = cur.fetchone()['count']
            
            # Count users working while studying (has job with start_date before graduation)
            cur.execute("""
                SELECT COUNT(DISTINCT e.user_id) as count
                FROM education ed
                JOIN experience e ON ed.user_id = e.user_id
                WHERE e.start_date IS NOT NULL
                AND e.start_date < COALESCE(ed.graduation_date, DATE(ed.graduation_year, 5, 15))
            """)
            working_while_studying = cur.fetchone()['count']
            
            # Count graduated (has graduation_date or graduation_year and not is_expected)
            cur.execute("""
                SELECT 
                    SUM(CASE WHEN graduation_year IS NOT NULL OR graduation_date IS NOT NULL THEN 1 ELSE 0 END) as graduated,
                    SUM(CASE WHEN is_expected = TRUE THEN 1 ELSE 0 END) as not_yet_graduated,
                    SUM(CASE WHEN graduation_year IS NULL AND graduation_date IS NULL THEN 1 ELSE 0 END) as unknown
                FROM education
            """)
            stats = cur.fetchone() or {}
            
            graduated = stats.get('graduated', 0) or 0
            not_yet_graduated = stats.get('not_yet_graduated', 0) or 0
            unknown = stats.get('unknown', 0) or 0
        
        percentage = (working_while_studying / total * 100) if total > 0 else 0
        
        return jsonify({
            "total_analyzed": total,
            "working_while_studying_count": working_while_studying,
            "graduated_count": graduated,
            "not_yet_graduated_count": not_yet_graduated,
            "unknown_status_count": unknown,
            "percentage_working_while_studying": round(percentage, 2)
        }), 200
    
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({
            "error": "Failed to retrieve statistics",
            "message": str(e)
        }), 500
    
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# SETUP/INITIALIZATION
# ============================================================

@work_analysis_bp.route('/init', methods=['POST'])
def initialize_schema():
    """
    Initialize education and experience tables.
    **Admin endpoint - should be protected with authentication**
    """
    try:
        # TODO: Add authentication check here
        # Example:
        # if not is_admin_user(request):
        #     return jsonify({"error": "Unauthorized"}), 403
        
        success = ensure_work_while_studying_schema(get_connection)
        
        if success:
            return jsonify({
                "message": "Schema initialized successfully",
                "tables_created": ["education", "experience"]
            }), 200
        else:
            return jsonify({
                "error": "Failed to initialize schema"
            }), 500
    
    except Exception as e:
        logger.error(f"Error initializing schema: {e}")
        return jsonify({
            "error": "Failed to initialize schema",
            "message": str(e)
        }), 500


# ============================================================
# INTEGRATION INSTRUCTIONS
# ============================================================

"""
How to integrate this into your Flask app:

1. In backend/app.py, add after your other Blueprint registrations:

    from work_analysis_blueprint import work_analysis_bp
    app.register_blueprint(work_analysis_bp)

2. Or import and register directly:

    from work_while_studying import computeWorkWhileStudying
    from database import get_connection
    
    @app.route('/api/work-analysis/user/<int:user_id>')
    def get_work_analysis(user_id):
        result = computeWorkWhileStudying(user_id, get_connection)
        if result is None:
            return jsonify({"error": "Failed to analyze"}), 500
        # Convert dates to ISO format strings...
        return jsonify(result)

3. Protect the /init endpoint with authentication:

    from functools import wraps
    
    def require_admin(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not is_admin_user(request):
                return jsonify({"error": "Unauthorized"}), 403
            return f(*args, **kwargs)
        return decorated
    
    @work_analysis_bp.route('/init', methods=['POST'])
    @require_admin
    def initialize_schema():
        ...

Available Endpoints:

GET  /api/work-analysis/user/<user_id>
     - Analyze single user
     - Returns: work_while_studying analysis

POST /api/work-analysis/batch
     - Analyze multiple users
     - Body: {"user_ids": [123, 456, ...]}
     - Returns: batch results with success/failure counts

GET  /api/work-analysis/stats
     - Get aggregate statistics
     - Returns: counts and percentages

POST /api/work-analysis/init
     - Initialize database schema
     - Admin endpoint (requires auth)
     - Returns: confirmation of tables created
"""
