import importlib
import sys

from flask import Blueprint, current_app, jsonify, request

from middleware import api_login_required
from utils import parse_int_list_param


interaction_bp = Blueprint("interaction", __name__)


def _app_mod():
	return sys.modules.get("app") or sys.modules.get("__main__") or importlib.import_module("app")


@interaction_bp.route("/api/interaction", methods=["POST"])
@api_login_required
def add_interaction():
	if current_app.config.get("DISABLE_DB") and not current_app.config.get("USE_SQLITE_FALLBACK"):
		return jsonify({"success": True, "message": "DB disabled (dev). No-op."}), 200

	data = request.get_json(silent=True) or {}
	alumni_id = data.get("alumni_id")
	interaction_type = data.get("interaction_type")
	notes = data.get("notes", "")

	if not alumni_id or not interaction_type:
		return jsonify({"error": "Missing alumni_id or interaction_type"}), 400
	if interaction_type not in ["bookmarked", "connected"]:
		return jsonify({"error": "Invalid interaction_type. Must be 'bookmarked' or 'connected'"}), 400

	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		if use_sqlite:
			cursor = conn.cursor()
			cursor.execute(
				"""
				INSERT INTO user_interactions (user_id, alumni_id, interaction_type, notes, created_at, updated_at)
				VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
				ON CONFLICT(user_id, alumni_id, interaction_type) DO UPDATE SET
					notes = excluded.notes,
					updated_at = datetime('now')
				""",
				(user_id, alumni_id, interaction_type, notes),
			)
			conn.commit()
		else:
			with conn.cursor() as cur:
				cur.execute(
					"""
					INSERT INTO user_interactions (user_id, alumni_id, interaction_type, notes)
					VALUES (%s, %s, %s, %s)
					ON DUPLICATE KEY UPDATE
						notes = VALUES(notes),
						updated_at = CURRENT_TIMESTAMP
					""",
					(user_id, alumni_id, interaction_type, notes),
				)
				conn.commit()

		return jsonify({"success": True, "message": f"{interaction_type} added successfully"}), 200
	except Exception as err:
		try:
			conn.rollback()
		except Exception:
			pass
		current_app.logger.error("Database error adding interaction: %s", err)
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/interaction", methods=["DELETE"])
@api_login_required
def remove_interaction():
	if current_app.config.get("DISABLE_DB") and not current_app.config.get("USE_SQLITE_FALLBACK"):
		return jsonify({"success": True, "message": "DB disabled (dev). No-op."}), 200

	data = request.get_json(silent=True) or {}
	alumni_id = data.get("alumni_id")
	interaction_type = data.get("interaction_type")

	if not alumni_id or not interaction_type:
		return jsonify({"error": "Missing alumni_id or interaction_type"}), 400

	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		if use_sqlite:
			cursor = conn.cursor()
			cursor.execute(
				"""
				DELETE FROM user_interactions
				WHERE user_id = ? AND alumni_id = ? AND interaction_type = ?
				""",
				(user_id, alumni_id, interaction_type),
			)
			conn.commit()
		else:
			with conn.cursor() as cur:
				cur.execute(
					"""
					DELETE FROM user_interactions
					WHERE user_id = %s AND alumni_id = %s AND interaction_type = %s
					""",
					(user_id, alumni_id, interaction_type),
				)
				conn.commit()

		return jsonify({"success": True, "message": "Interaction removed"}), 200
	except Exception as err:
		try:
			conn.rollback()
		except Exception:
			pass
		current_app.logger.error("Database error removing interaction: %s", err)
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/user-interactions", methods=["GET"])
@api_login_required
def get_user_interactions():
	if current_app.config.get("DISABLE_DB") and not current_app.config.get("USE_SQLITE_FALLBACK"):
		return jsonify({"success": True, "interactions": [], "count": 0, "bookmarked_total": 0}), 200

	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	alumni_ids = parse_int_list_param(request, "alumni_ids")[:1000]
	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")

	try:
		interactions = []
		bookmarked_total = 0

		if use_sqlite:
			with conn.cursor(dictionary=True) as cursor:
				cursor.execute(
					"""
					SELECT COUNT(*) AS bookmarked_total
					FROM user_interactions
					WHERE user_id = ? AND interaction_type = 'bookmarked'
					""",
					(user_id,),
				)
				if hasattr(cursor, "fetchone"):
					count_row = cursor.fetchone() or {}
				else:
					rows = cursor.fetchall() or []
					count_row = rows[0] if rows else {}
				bookmarked_total = count_row.get("bookmarked_total", 0) or 0

				sql = """
					SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
					FROM user_interactions
					WHERE user_id = ?
				"""
				params = [user_id]
				if alumni_ids:
					placeholders = ",".join(["?"] * len(alumni_ids))
					sql += f" AND alumni_id IN ({placeholders})"
					params.extend(alumni_ids)
				sql += " ORDER BY updated_at DESC"
				cursor.execute(sql, tuple(params))
				interactions = cursor.fetchall() or []
		else:
			with conn.cursor(dictionary=True) as cur:
				cur.execute(
					"""
					SELECT COUNT(*) AS bookmarked_total
					FROM user_interactions
					WHERE user_id = %s AND interaction_type = 'bookmarked'
					""",
					(user_id,),
				)
				if hasattr(cur, "fetchone"):
					count_row = cur.fetchone() or {}
				else:
					rows = cur.fetchall() or []
					count_row = rows[0] if rows else {}
				bookmarked_total = count_row.get("bookmarked_total", 0) or 0

				sql = """
					SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
					FROM user_interactions
					WHERE user_id = %s
				"""
				params = [user_id]
				if alumni_ids:
					placeholders = ",".join(["%s"] * len(alumni_ids))
					sql += f" AND alumni_id IN ({placeholders})"
					params.extend(alumni_ids)
				sql += " ORDER BY updated_at DESC"
				cur.execute(sql, tuple(params))
				interactions = cur.fetchall() or []

		for interaction in interactions:
			created_at = interaction.get("created_at")
			updated_at = interaction.get("updated_at")
			if hasattr(created_at, "isoformat"):
				interaction["created_at"] = created_at.isoformat()
			if hasattr(updated_at, "isoformat"):
				interaction["updated_at"] = updated_at.isoformat()

		return jsonify(
			{
				"success": True,
				"interactions": interactions,
				"count": len(interactions),
				"bookmarked_total": bookmarked_total,
			}
		), 200
	except Exception as err:
		current_app.logger.error("Database error getting user interactions: %s", err)
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/notes/<int:alumni_id>", methods=["GET"])
@api_login_required
def api_get_note(alumni_id):
	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		with conn.cursor(dictionary=True) as cur:
			if use_sqlite:
				cur.execute(
					"SELECT note, updated_at FROM notes WHERE user_id = ? AND alumni_id = ?",
					(user_id, alumni_id),
				)
			else:
				cur.execute(
					"SELECT note, updated_at FROM notes WHERE user_id = %s AND alumni_id = %s",
					(user_id, alumni_id),
				)
			row = cur.fetchone() or {}
		return jsonify({"success": True, "note": row.get("note", ""), "updated_at": row.get("updated_at")}), 200
	except Exception as err:
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/notes/<int:alumni_id>", methods=["POST"])
@api_login_required
def api_upsert_note(alumni_id):
	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	data = request.get_json(silent=True) or {}
	note = (data.get("note") or "").strip()

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		with conn.cursor() as cur:
			if use_sqlite:
				cur.execute(
					"""
					INSERT INTO notes (user_id, alumni_id, note, created_at, updated_at)
					VALUES (?, ?, ?, datetime('now'), datetime('now'))
					ON CONFLICT(user_id, alumni_id) DO UPDATE SET
						note = excluded.note,
						updated_at = datetime('now')
					""",
					(user_id, alumni_id, note),
				)
			else:
				cur.execute(
					"""
					INSERT INTO notes (user_id, alumni_id, note)
					VALUES (%s, %s, %s)
					ON DUPLICATE KEY UPDATE
						note = VALUES(note),
						updated_at = CURRENT_TIMESTAMP
					""",
					(user_id, alumni_id, note),
				)
			conn.commit()

		return jsonify({"success": True, "message": "Note saved."}), 200
	except Exception as err:
		try:
			conn.rollback()
		except Exception:
			pass
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/notes/<int:alumni_id>", methods=["DELETE"])
@api_login_required
def api_delete_note(alumni_id):
	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		with conn.cursor() as cur:
			if use_sqlite:
				cur.execute("DELETE FROM notes WHERE user_id = ? AND alumni_id = ?", (user_id, alumni_id))
			else:
				cur.execute("DELETE FROM notes WHERE user_id = %s AND alumni_id = %s", (user_id, alumni_id))
			conn.commit()

		return jsonify({"success": True, "message": "Note deleted."}), 200
	except Exception as err:
		try:
			conn.rollback()
		except Exception:
			pass
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/notes", methods=["GET"])
@api_login_required
def get_all_notes():
	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	if current_app.config.get("DISABLE_DB") and not current_app.config.get("USE_SQLITE_FALLBACK"):
		return jsonify({"success": True, "notes": {}, "count": 0}), 200

	conn = _app_mod().get_connection()
	use_sqlite = current_app.config.get("DISABLE_DB") and current_app.config.get("USE_SQLITE_FALLBACK")
	try:
		rows = []
		if use_sqlite:
			with conn.cursor(dictionary=True) as cursor:
				cursor.execute(
					"""
					SELECT id, alumni_id, note_content, created_at, updated_at
					FROM notes
					WHERE user_id = ?
					ORDER BY updated_at DESC
					""",
					(user_id,),
				)
				rows = cursor.fetchall() or []
		else:
			with conn.cursor(dictionary=True) as cur:
				cur.execute(
					"""
					SELECT id, alumni_id, note_content, created_at, updated_at
					FROM notes
					WHERE user_id = %s
					ORDER BY updated_at DESC
					""",
					(user_id,),
				)
				rows = cur.fetchall() or []

		notes_by_alumni = {}
		for note in rows:
			alumni_id = note.get("alumni_id")
			if alumni_id is None:
				continue
			created_at = note.get("created_at")
			updated_at = note.get("updated_at")
			if hasattr(created_at, "isoformat"):
				created_at = created_at.isoformat()
			if hasattr(updated_at, "isoformat"):
				updated_at = updated_at.isoformat()

			notes_by_alumni[alumni_id] = {
				"id": note.get("id"),
				"alumni_id": alumni_id,
				"note_content": note.get("note_content") or "",
				"created_at": created_at,
				"updated_at": updated_at,
			}

		return jsonify({"success": True, "notes": notes_by_alumni, "count": len(notes_by_alumni)}), 200
	except Exception as err:
		current_app.logger.error("Database error getting all notes: %s", err)
		return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass


@interaction_bp.route("/api/notes/summary", methods=["GET"])
@api_login_required
def api_notes_summary():
	user_id = _app_mod().get_current_user_id()
	if not user_id:
		return jsonify({"error": "User not found"}), 401

	ids = parse_int_list_param(request, "ids")
	if not ids:
		return jsonify({"success": True, "summary": {}, "count": 0}), 200

	conn = _app_mod().get_connection()
	try:
		with conn.cursor(dictionary=True) as cur:
			placeholders = ",".join(["%s"] * len(ids))
			cur.execute(
				f"SELECT alumni_id FROM notes WHERE user_id = %s AND alumni_id IN ({placeholders})",
				tuple([user_id] + ids),
			)
			rows = cur.fetchall() or []
			present = {str(int(r.get("alumni_id"))) for r in rows if r.get("alumni_id") is not None}

		summary = {str(i): (str(i) in present) for i in ids}
		return jsonify({"success": True, "summary": summary, "count": len(present)}), 200
	except Exception as err:
		return jsonify({"error": f"Database error: {str(err)}"}), 500
	finally:
		try:
			conn.close()
		except Exception:
			pass
