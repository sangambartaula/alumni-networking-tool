// Settings modal management for alumni.html
// Handles: Email Whitelist, Scraper Activity, Password Management, Admin User Management

document.addEventListener('DOMContentLoaded', function () {
  // Modal elements
  const settingsModal = document.getElementById('settingsModal');
  const addEmailModal = document.getElementById('addEmailModal');
  const confirmModal = document.getElementById('confirmModal');

  // Button elements
  const settingsBtn = document.getElementById('settingsBtn');
  const closeSettingsModal = document.getElementById('closeSettingsModal');
  const addEmailBtn = document.getElementById('addEmailBtn');
  const closeAddEmailModal = document.getElementById('closeAddEmailModal');
  const confirmAddEmail = document.getElementById('confirmAddEmail');
  const cancelAddEmail = document.getElementById('cancelAddEmail');
  const confirmYes = document.getElementById('confirmYes');
  const confirmNo = document.getElementById('confirmNo');

  // Email list
  const emailList = document.getElementById('emailList');
  const newEmailInput = document.getElementById('newEmail');
  const emailNotesInput = document.getElementById('emailNotes');
  const confirmMessage = document.getElementById('confirmMessage');
  const scraperActivityBody = document.getElementById('scraperActivityBody');
  const scraperActivityTotal = document.getElementById('scraperActivityTotal');

  // Temporary storage for email being added
  let pendingEmail = null;
  let pendingNotes = null;

  // ──────────── Current User State ────────────
  let currentUser = null;

  async function fetchCurrentUser() {
    try {
      const resp = await fetch('/api/auth/me');
      if (!resp.ok) return;
      currentUser = await resp.json();
      renderRoleSections();
    } catch (e) {
      console.error('Error fetching user info:', e);
    }
  }

  function renderRoleSections() {
    // Password management section
    const pwSection = document.getElementById('passwordSection');
    if (pwSection && currentUser) {
      pwSection.style.display = 'block';
      const hasPassword = currentUser.auth_type === 'email_password' || currentUser.auth_type === 'both';
      document.getElementById('changePwGroup').style.display = hasPassword ? 'block' : 'none';
      document.getElementById('createPwGroup').style.display = hasPassword ? 'none' : 'block';
    }

    // Admin section
    const adminSection = document.getElementById('adminSection');
    if (adminSection) {
      adminSection.style.display = (currentUser && currentUser.role === 'admin') ? 'block' : 'none';
      if (currentUser && currentUser.role === 'admin') loadAdminUsers();
    }
  }

  const emailWhitelistSection = document.getElementById('emailWhitelistSection');

  // Open settings modal
  settingsBtn.addEventListener('click', async function () {
    settingsModal.style.display = 'block';
    await fetchCurrentUser();
    if (currentUser && currentUser.role === 'admin') {
      if (emailWhitelistSection) emailWhitelistSection.style.display = 'block';
      loadAuthorizedEmails();
    } else {
      if (emailWhitelistSection) emailWhitelistSection.style.display = 'none';
      if (emailList) {
        emailList.innerHTML = '<li class="empty">Email whitelist is managed by administrators.</li>';
      }
    }
    loadScraperActivity();
  });

  // Close settings modal
  closeSettingsModal.addEventListener('click', function () {
    settingsModal.style.display = 'none';
  });

  // Close modal when clicking overlay
  settingsModal.querySelector('.modal-overlay').addEventListener('click', function () {
    settingsModal.style.display = 'none';
  });

  // Open add email modal
  addEmailBtn.addEventListener('click', function () {
    addEmailModal.style.display = 'block';
    newEmailInput.value = '';
    emailNotesInput.value = '';
    newEmailInput.focus();
  });

  // Close add email modal
  closeAddEmailModal.addEventListener('click', function () {
    addEmailModal.style.display = 'none';
  });

  cancelAddEmail.addEventListener('click', function () {
    addEmailModal.style.display = 'none';
  });

  addEmailModal.querySelector('.modal-overlay').addEventListener('click', function () {
    addEmailModal.style.display = 'none';
  });

  // Confirm add email - show confirmation
  confirmAddEmail.addEventListener('click', function () {
    const email = newEmailInput.value.trim();
    const notes = emailNotesInput.value.trim();

    if (!email) {
      alert('Please enter an email address');
      return;
    }

    // Basic email validation
    if (!email.includes('@') || !email.split('@')[1].includes('.')) {
      alert('Please enter a valid email address');
      return;
    }

    // Store pending email and show confirmation
    pendingEmail = email;
    pendingNotes = notes;
    confirmMessage.textContent = `Are you sure? ${email} will have access to this website from now on.`;

    addEmailModal.style.display = 'none';
    confirmModal.style.display = 'block';
  });

  // Cancel confirmation
  confirmNo.addEventListener('click', function () {
    confirmModal.style.display = 'none';
    pendingEmail = null;
    pendingNotes = null;
  });

  confirmModal.querySelector('.modal-overlay').addEventListener('click', function () {
    confirmModal.style.display = 'none';
    pendingEmail = null;
    pendingNotes = null;
  });

  // Confirm and add email
  confirmYes.addEventListener('click', async function () {
    if (!pendingEmail) return;

    try {
      const response = await fetch('/api/authorized-emails', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          email: pendingEmail,
          notes: pendingNotes
        })
      });

      const data = await response.json();

      if (data.success) {
        confirmModal.style.display = 'none';
        pendingEmail = null;
        pendingNotes = null;
        loadAuthorizedEmails();
      } else {
        alert('Error adding email: ' + (data.error || 'Unknown error'));
      }
    } catch (error) {
      console.error('Error adding email:', error);
      alert('Error adding email. Please try again.');
    }
  });

  // Load authorized emails from API
  async function loadAuthorizedEmails() {
    if (!emailList) return;
    if (!currentUser || currentUser.role !== 'admin') {
      return;
    }
    try {
      const response = await fetch('/api/authorized-emails');
      let data = null;
      try {
        data = await response.json();
      } catch (parseErr) {
        console.error('Invalid JSON from authorized-emails:', parseErr);
        emailList.innerHTML = '<li class="error">Error loading emails (invalid server response)</li>';
        return;
      }

      if (response.ok && data.success) {
        displayEmails(data.emails);
      } else {
        const msg = (data && (data.error || data.message)) || response.statusText || 'Request failed';
        emailList.innerHTML = `<li class="error">Could not load whitelist: ${escapeHtml(msg)}</li>`;
      }
    } catch (error) {
      console.error('Error fetching emails:', error);
      emailList.innerHTML = '<li class="error">Error loading emails</li>';
    }
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatRelativeTime(value) {
    if (!value) return 'Never';

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return 'Unknown';

    const diffMs = Date.now() - parsed.getTime();
    const diffSeconds = Math.floor(diffMs / 1000);
    if (diffSeconds < 60) return 'Just now';

    const diffMinutes = Math.floor(diffSeconds / 60);
    if (diffMinutes < 60) return `${diffMinutes}m ago`;

    const diffHours = Math.floor(diffMinutes / 60);
    if (diffHours < 24) return `${diffHours}h ago`;

    const diffDays = Math.floor(diffHours / 24);
    if (diffDays < 30) return `${diffDays}d ago`;

    return parsed.toLocaleDateString();
  }

  async function loadScraperActivity() {
    if (!scraperActivityBody || !scraperActivityTotal) return;

    try {
      const response = await fetch('/api/scraper-activity');
      const data = await response.json();
      const rows = Array.isArray(data.activity) ? data.activity : [];
      const total = Number.isFinite(data.total_profiles_scraped)
        ? data.total_profiles_scraped
        : rows.reduce((sum, row) => sum + (parseInt(row.profiles_scraped, 10) || 0), 0);

      scraperActivityTotal.textContent = `Total scraped: ${total}`;

      if (!rows.length) {
        scraperActivityBody.textContent = 'No scraper activity recorded yet.';
        return;
      }

      const tableRows = rows.map((row) => {
        const displayName = escapeHtml(row.display_name || row.email || 'Unknown Scraper');
        const count = parseInt(row.profiles_scraped, 10) || 0;
        const lastSeen = escapeHtml(formatRelativeTime(row.last_scraped_at));
        return `<tr><td>${displayName}</td><td>${count}</td><td>${lastSeen}</td></tr>`;
      }).join('');

      scraperActivityBody.innerHTML = `
        <table class="scraper-activity-table">
          <thead>
            <tr>
              <th>Scraper</th>
              <th>Profiles</th>
              <th>Last Scraped</th>
            </tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      `;
    } catch (error) {
      console.error('Error loading scraper activity:', error);
      scraperActivityTotal.textContent = 'Total scraped: unavailable';
      scraperActivityBody.textContent = 'Scraper activity is unavailable right now.';
    }
  }

  // Display emails in the list
  function displayEmails(emails) {
    if (!emails || emails.length === 0) {
      emailList.innerHTML = '<li class="empty">No emails in whitelist</li>';
      return;
    }

    emailList.innerHTML = '';
    emails.forEach(function (emailRecord) {
      const li = document.createElement('li');
      li.className = 'email-item';

      const emailInfo = document.createElement('div');
      emailInfo.className = 'email-info';

      const emailText = document.createElement('span');
      emailText.className = 'email-text';
      emailText.textContent = emailRecord.email;
      emailInfo.appendChild(emailText);

      if (emailRecord.notes) {
        const noteText = document.createElement('small');
        noteText.className = 'email-note';
        noteText.textContent = emailRecord.notes;
        emailInfo.appendChild(noteText);
      }

      const deleteBtn = document.createElement('button');
      deleteBtn.className = 'btn-delete';
      deleteBtn.title = 'Remove email';
      deleteBtn.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="3 6 5 6 21 6"></polyline>
                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                </svg>
            `;
      deleteBtn.addEventListener('click', function () {
        deleteEmail(emailRecord.email);
      });

      li.appendChild(emailInfo);
      li.appendChild(deleteBtn);
      emailList.appendChild(li);
    });
  }

  // Delete email
  async function deleteEmail(email) {
    if (!confirm(`Remove ${email} from whitelist?`)) {
      return;
    }

    try {
      const response = await fetch('/api/authorized-emails', {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ email: email })
      });

      const data = await response.json();

      if (data.success) {
        loadAuthorizedEmails();
      } else {
        alert('Error removing email: ' + (data.error || 'Unknown error'));
      }
    } catch (error) {
      console.error('Error removing email:', error);
      alert('Error removing email. Please try again.');
    }
  }


  // ──────────── Password Management ────────────

  // Change password (user has existing password)
  const changePwBtn = document.getElementById('changePwBtn');
  if (changePwBtn) {
    changePwBtn.addEventListener('click', async function () {
      const currentPw = document.getElementById('currentPw').value;
      const newPw = document.getElementById('newPw').value;
      const statusEl = document.getElementById('changePwStatus');

      if (!currentPw || !newPw) {
        statusEl.textContent = 'Both fields are required.';
        statusEl.className = 'field-status error';
        return;
      }

      try {
        const resp = await fetch('/api/auth/change-password', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ current_password: currentPw, new_password: newPw })
        });
        const data = await resp.json();
        if (data.success) {
          statusEl.textContent = '✓ Password changed.';
          statusEl.className = 'field-status success';
          document.getElementById('currentPw').value = '';
          document.getElementById('newPw').value = '';
        } else {
          let msg = data.error || 'Failed.';
          if (data.details) msg += ' ' + data.details.join(' ');
          statusEl.textContent = msg;
          statusEl.className = 'field-status error';
        }
      } catch (e) {
        statusEl.textContent = 'Network error.';
        statusEl.className = 'field-status error';
      }
    });
  }

  // Create password (LinkedIn-only users)
  const createPwBtn = document.getElementById('createPwBtn');
  if (createPwBtn) {
    createPwBtn.addEventListener('click', async function () {
      const newPw = document.getElementById('createNewPw').value;
      const statusEl = document.getElementById('createPwStatus');

      if (!newPw) {
        statusEl.textContent = 'Password is required.';
        statusEl.className = 'field-status error';
        return;
      }

      try {
        const resp = await fetch('/api/auth/create-password', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ new_password: newPw })
        });
        const data = await resp.json();
        if (data.success) {
          statusEl.textContent = '✓ Password created.';
          statusEl.className = 'field-status success';
          document.getElementById('createNewPw').value = '';
          // Refresh sections
          fetchCurrentUser();
        } else {
          let msg = data.error || 'Failed.';
          if (data.details) msg += ' ' + data.details.join(' ');
          statusEl.textContent = msg;
          statusEl.className = 'field-status error';
        }
      } catch (e) {
        statusEl.textContent = 'Network error.';
        statusEl.className = 'field-status error';
      }
    });
  }


  // ──────────── Admin: User Management ────────────

  async function loadAdminUsers() {
    const tbody = document.getElementById('adminUsersList');
    if (!tbody) return;

    try {
      const resp = await fetch('/api/admin/users');
      const data = await resp.json();
      if (!data.success) { tbody.textContent = 'Error loading users.'; return; }

      const users = data.users || [];
      if (!users.length) { tbody.innerHTML = '<tr><td colspan="4">No users found.</td></tr>'; return; }

      tbody.innerHTML = users.map(u => {
        const email = escapeHtml(u.email);
        const role = escapeHtml(u.role || 'user');
        const authType = escapeHtml(u.auth_type || '');
        const isSelf = currentUser && currentUser.email === u.email;
        return `<tr>
          <td>${email}</td>
          <td>
            <select data-email="${email}" class="admin-role-select" ${isSelf ? 'disabled' : ''}>
              <option value="user" ${role === 'user' ? 'selected' : ''}>User</option>
              <option value="admin" ${role === 'admin' ? 'selected' : ''}>Admin</option>
            </select>
          </td>
          <td>${authType}</td>
          <td class="admin-actions">
            <button class="btn-sm btn-reset" data-email="${email}" title="Reset password">Reset PW</button>
            <button class="btn-sm btn-danger" data-email="${email}" ${isSelf ? 'disabled' : ''} title="Delete user">Delete</button>
          </td>
        </tr>`;
      }).join('');

      // Role change
      tbody.querySelectorAll('.admin-role-select').forEach(sel => {
        sel.addEventListener('change', async function () {
          const email = this.dataset.email;
          const role = this.value;
          try {
            const resp = await fetch('/api/admin/users/role', {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email, role })
            });
            const data = await resp.json();
            if (!data.success) alert(data.error || 'Failed to update role.');
          } catch (e) { alert('Network error.'); }
        });
      });

      // Reset password
      tbody.querySelectorAll('.btn-reset').forEach(btn => {
        btn.addEventListener('click', async function () {
          const email = this.dataset.email;
          if (!confirm(`Reset password for ${email}? They will need to set a new one.`)) return;
          try {
            const resp = await fetch('/api/admin/users/reset-password', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email })
            });
            const data = await resp.json();
            alert(data.message || data.error || 'Done.');
          } catch (e) { alert('Network error.'); }
        });
      });

      // Delete user
      tbody.querySelectorAll('.btn-danger').forEach(btn => {
        btn.addEventListener('click', async function () {
          const email = this.dataset.email;
          if (!confirm(`Delete user ${email}? This cannot be undone.`)) return;
          try {
            const resp = await fetch('/api/admin/users', {
              method: 'DELETE',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email })
            });
            const data = await resp.json();
            if (data.success) loadAdminUsers();
            else alert(data.error || 'Failed to delete.');
          } catch (e) { alert('Network error.'); }
        });
      });

    } catch (e) {
      console.error('Error loading admin users:', e);
      tbody.textContent = 'Error loading users.';
    }
  }

  // Admin: add user
  const adminAddUserBtn = document.getElementById('adminAddUserBtn');
  if (adminAddUserBtn) {
    adminAddUserBtn.addEventListener('click', async function () {
      const email = document.getElementById('adminNewUserEmail').value.trim();
      const role = document.getElementById('adminNewUserRole').value;
      const statusEl = document.getElementById('adminAddUserStatus');

      if (!email) {
        statusEl.textContent = 'Email is required.';
        statusEl.className = 'field-status error';
        return;
      }

      try {
        const resp = await fetch('/api/admin/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, role })
        });
        const data = await resp.json();
        if (data.success) {
          statusEl.textContent = '✓ ' + (data.message || 'User added.');
          statusEl.className = 'field-status success';
          document.getElementById('adminNewUserEmail').value = '';
          loadAdminUsers();
          loadAuthorizedEmails();
        } else {
          statusEl.textContent = data.error || 'Failed.';
          statusEl.className = 'field-status error';
        }
      } catch (e) {
        statusEl.textContent = 'Network error.';
        statusEl.className = 'field-status error';
      }
    });
  }
});
