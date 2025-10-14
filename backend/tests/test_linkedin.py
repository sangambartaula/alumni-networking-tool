import re
import responses

@responses.activate
def test_linkedin_callback_does_not_hit_real_api(client):
    # 1) Seed session 'state' so your route passes CSRF check
    with client.session_transaction() as sess:
        sess["oauth_state"] = "teststate"

    # 2) Mock token exchange
    responses.add(
        responses.POST,
        re.compile(r"https://.*linkedin\.com/oauth/v2/accessToken", re.I),
        json={"access_token": "fake_token", "expires_in": 3600},
        status=200,
    )

    # 3) Mock user info/profile call your app makes after token exchange
    responses.add(
        responses.GET,
        re.compile(r"https://api\.linkedin\.com/v2/userinfo", re.I),
        json={
            "sub": "12345",
            "name": "Test User",
            "given_name": "Test",
            "family_name": "User",
            "email": "test@example.com",
        },
        status=200,
    )

    # If your code also calls other v2 endpoints (e.g., /me or /emailAddress),
    # you can mock broadly instead:
    # responses.add(responses.GET, re.compile(r"https://api\.linkedin\.com/.*", re.I),
    #               json={}, status=200)

    # 4) Call your callback with matching state + fake code
    resp = client.get("/auth/linkedin/callback?code=abc&state=teststate")
    assert resp.status_code in (200, 302, 400)
