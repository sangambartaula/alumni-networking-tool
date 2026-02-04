// Settings modal management for alumni.html

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

  // Temporary storage for email being added
  let pendingEmail = null;
  let pendingNotes = null;

  // Open settings modal
  settingsBtn.addEventListener('click', function () {
    settingsModal.style.display = 'block';
    loadAuthorizedEmails();
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
    try {
      const response = await fetch('/api/authorized-emails');
      const data = await response.json();

      if (data.success) {
        displayEmails(data.emails);
      } else {
        emailList.innerHTML = '<li class="error">Error loading emails</li>';
      }
    } catch (error) {
      console.error('Error fetching emails:', error);
      emailList.innerHTML = '<li class="error">Error loading emails</li>';
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
});
