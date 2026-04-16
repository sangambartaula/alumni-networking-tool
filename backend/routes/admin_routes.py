from flask import Blueprint, jsonify, request

from database import (
	add_authorized_email,
	admin_reset_password,
	create_user_with_password,
	delete_user,
	get_all_users,
	get_authorized_emails,
	get_user_by_email,
	remove_authorized_email,
	set_must_change_password,
	update_user_role,
)
from middleware import (
	_get_session_email,
	admin_required,
	api_login_required,
	get_current_user_id,
)


admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/api/admin/users", methods=["GET"])
@api_login_required
@admin_required
def api_admin_get_users():
	users = get_all_users()
	return jsonify({"success": True, "users": users}), 200


@admin_bp.route("/api/admin/users", methods=["POST"])
@api_login_required
@admin_required
def api_admin_add_user():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()
	role = data.get("role", "user")

	if not email:
		return jsonify({"error": "Email is required."}), 400
	if role not in ("admin", "user"):
		return jsonify({"error": "Role must be 'admin' or 'user'."}), 400

	existing = get_user_by_email(email)
	if existing:
		return jsonify({"error": f"User {email} already exists."}), 409

	add_authorized_email(email, added_by_user_id=get_current_user_id(), notes="Added by admin")
	success = create_user_with_password(email, None, role=role)
	if not success:
		return jsonify({"error": "Failed to create user."}), 500

	set_must_change_password(email, True)
	return jsonify({"success": True, "message": f"User {email} created with role {role}."}), 201


@admin_bp.route("/api/admin/users", methods=["DELETE"])
@api_login_required
@admin_required
def api_admin_delete_user():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()
	if not email:
		return jsonify({"error": "Email is required."}), 400

	if email == _get_session_email():
		return jsonify({"error": "Cannot delete your own account."}), 400

	success = delete_user(email)
	if not success:
		return jsonify({"error": "Failed to delete user."}), 500

	return jsonify({"success": True, "message": f"User {email} deleted."}), 200


@admin_bp.route("/api/admin/users/role", methods=["PUT"])
@api_login_required
@admin_required
def api_admin_update_role():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()
	role = data.get("role", "")

	if not email:
		return jsonify({"error": "Email is required."}), 400
	if role not in ("admin", "user"):
		return jsonify({"error": "Role must be 'admin' or 'user'."}), 400

	success = update_user_role(email, role)
	if not success:
		return jsonify({"error": "Failed to update role."}), 500

	return jsonify({"success": True, "message": f"{email} is now {role}."}), 200


@admin_bp.route("/api/admin/users/reset-password", methods=["POST"])
@api_login_required
@admin_required
def api_admin_reset_password():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()
	if not email:
		return jsonify({"error": "Email is required."}), 400

	success = admin_reset_password(email)
	if not success:
		return jsonify({"error": "Failed to reset password."}), 500

	return jsonify(
		{
			"success": True,
			"message": f"Password reset for {email}. They must set a new password on next login.",
		}
	), 200


@admin_bp.route("/api/authorized-emails", methods=["GET"])
@api_login_required
@admin_required
def get_authorized_emails_api():
	emails = get_authorized_emails()
	return jsonify({"success": True, "emails": emails}), 200


@admin_bp.route("/api/authorized-emails", methods=["POST"])
@api_login_required
@admin_required
def add_authorized_email_api():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()
	notes = (data.get("notes") or "").strip() or None

	if not email:
		return jsonify({"error": "Email is required"}), 400

	user_id = get_current_user_id()
	success = add_authorized_email(email, added_by_user_id=user_id, notes=notes)
	if success:
		return jsonify({"success": True, "message": f"Email {email} added to whitelist"}), 200
	return jsonify({"error": "Failed to add email"}), 500


@admin_bp.route("/api/authorized-emails", methods=["DELETE"])
@api_login_required
@admin_required
def remove_authorized_email_api():
	data = request.get_json(silent=True) or {}
	email = (data.get("email") or "").strip().lower()

	if not email:
		return jsonify({"error": "Email is required"}), 400

	success = remove_authorized_email(email)
	if success:
		return jsonify({"success": True, "message": f"Email {email} removed from whitelist"}), 200
	return jsonify({"error": "Failed to remove email"}), 500
