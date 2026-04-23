(function () {
  const COOKIE_NAME = 'privacy_mode_redact_names';
  const STORAGE_KEY = 'privacy_mode_redact_names';
  const HIDDEN_LABEL = 'Name Hidden';
  const COOKIE_MAX_AGE = 60 * 60 * 24 * 365; // 1 year

  function readCookie(name) {
    const target = `${name}=`;
    const parts = document.cookie ? document.cookie.split(';') : [];
    for (let i = 0; i < parts.length; i += 1) {
      const value = parts[i].trim();
      if (value.startsWith(target)) {
        return decodeURIComponent(value.slice(target.length));
      }
    }
    return null;
  }

  function writeCookie(name, value) {
    document.cookie = `${name}=${encodeURIComponent(value)}; path=/; max-age=${COOKIE_MAX_AGE}; samesite=lax`;
  }

  function normalizeBoolean(value) {
    return value === true || value === '1' || value === 'true' || value === 'yes' || value === 1;
  }

  function isEnabled() {
    const fromStorage = window.localStorage.getItem(STORAGE_KEY);
    if (fromStorage != null) return normalizeBoolean(fromStorage);
    const fromCookie = readCookie(COOKIE_NAME);
    if (fromCookie != null) return normalizeBoolean(fromCookie);
    return false;
  }

  function setEnabled(nextValue) {
    const enabled = !!nextValue;
    const persisted = enabled ? '1' : '0';
    window.localStorage.setItem(STORAGE_KEY, persisted);
    writeCookie(COOKIE_NAME, persisted);
    return enabled;
  }

  function getDisplayName(nameValue) {
    if (isEnabled()) return HIDDEN_LABEL;
    return String(nameValue || '').trim();
  }

  window.PrivacyMode = {
    isEnabled,
    setEnabled,
    getDisplayName,
    hiddenLabel: HIDDEN_LABEL,
  };
})();
