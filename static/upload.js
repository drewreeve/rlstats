(function () {
    var authSection = document.getElementById("auth-section");
    var uploadSection = document.getElementById("upload-section");
    var authForm = document.getElementById("auth-form");
    var authPassword = document.getElementById("auth-password");
    var authError = document.getElementById("auth-error");
    var dropZone = document.getElementById("drop-zone");
    var fileInput = document.getElementById("file-input");
    var fileList = document.getElementById("file-list");

    var MIN_SIZE = 256 * 1024;
    var MAX_SIZE = 3 * 1024 * 1024;
    var csrfToken = "";

    function showAuth() {
        authSection.hidden = false;
        uploadSection.hidden = true;
    }

    function showUpload() {
        authSection.hidden = true;
        uploadSection.hidden = false;
    }

    function showAuthError(msg) {
        authError.textContent = msg;
        authError.hidden = false;
    }

    // Check auth status on load
    fetch("/api/auth/status")
        .then(function (r) { return r.json(); })
        .then(function (data) {
            if (data.csrf_token) csrfToken = data.csrf_token;
            if (data.authenticated) showUpload();
            else showAuth();
        })
        .catch(function () { showAuth(); });

    // Auth form submit
    authForm.addEventListener("submit", function (e) {
        e.preventDefault();
        authError.hidden = true;
        var password = authPassword.value;
        if (!password) {
            showAuthError("Password required");
            return;
        }
        fetch("/api/auth", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
            body: JSON.stringify({ password: password })
        })
            .then(function (r) {
                if (r.ok) return r.json();
                return r.json().then(function (d) { throw new Error(d.error || "Authentication failed"); });
            })
            .then(function (data) {
                if (data.authenticated) showUpload();
                else showAuthError("Authentication failed");
            })
            .catch(function (err) {
                showAuthError(err.message || "Authentication failed");
            });
    });

    // Drop zone events
    dropZone.addEventListener("click", function () {
        fileInput.click();
    });

    dropZone.addEventListener("dragover", function (e) {
        e.preventDefault();
        dropZone.classList.add("drag-over");
    });

    dropZone.addEventListener("dragleave", function () {
        dropZone.classList.remove("drag-over");
    });

    dropZone.addEventListener("drop", function (e) {
        e.preventDefault();
        dropZone.classList.remove("drag-over");
        handleFiles(e.dataTransfer.files);
    });

    fileInput.addEventListener("change", function () {
        handleFiles(fileInput.files);
        fileInput.value = "";
    });

    function handleFiles(files) {
        for (var i = 0; i < files.length; i++) {
            processFile(files[i]);
        }
    }

    function processFile(file) {
        var entry = createFileEntry(file.name);
        fileList.prepend(entry);

        // Client-side validation
        if (!file.name.endsWith(".replay")) {
            setEntryStatus(entry, "error", "NOT A .REPLAY FILE");
            return;
        }
        if (file.size < MIN_SIZE) {
            setEntryStatus(entry, "error", "TOO SMALL (" + formatSize(file.size) + ")");
            return;
        }
        if (file.size > MAX_SIZE) {
            setEntryStatus(entry, "error", "TOO LARGE (" + formatSize(file.size) + ")");
            return;
        }

        uploadFile(file, entry);
    }

    function createFileEntry(name) {
        var entry = document.createElement("div");
        entry.className = "file-entry";

        var info = document.createElement("div");
        info.className = "file-entry-info";

        var nameEl = document.createElement("span");
        nameEl.className = "file-entry-name";
        nameEl.textContent = name;

        var statusEl = document.createElement("span");
        statusEl.className = "file-entry-status";
        statusEl.textContent = "UPLOADING...";

        info.appendChild(nameEl);
        info.appendChild(statusEl);

        var track = document.createElement("div");
        track.className = "file-progress-track";
        var bar = document.createElement("div");
        bar.className = "file-progress-bar";
        track.appendChild(bar);

        entry.appendChild(info);
        entry.appendChild(track);

        return entry;
    }

    function setEntryStatus(entry, status, text) {
        entry.setAttribute("data-status", status);
        var statusEl = entry.querySelector(".file-entry-status");
        statusEl.textContent = text;
        var bar = entry.querySelector(".file-progress-bar");
        if (status === "success") {
            bar.style.width = "100%";
            bar.classList.add("complete");
        } else if (status === "error" || status === "duplicate") {
            bar.style.width = "100%";
        }
    }

    function uploadFile(file, entry) {
        var xhr = new XMLHttpRequest();
        var formData = new FormData();
        formData.append("file", file);

        var bar = entry.querySelector(".file-progress-bar");

        xhr.upload.onprogress = function (e) {
            if (e.lengthComputable) {
                var pct = (e.loaded / e.total) * 100;
                bar.style.width = pct + "%";
            }
        };

        xhr.onload = function () {
            var data;
            try { data = JSON.parse(xhr.responseText); } catch (e) { data = {}; }

            if (xhr.status === 201) {
                setEntryStatus(entry, "processing", "PROCESSING...");
                pollStatus(data.filename, entry);
            } else if (xhr.status === 409) {
                setEntryStatus(entry, "duplicate", "DUPLICATE");
            } else if (xhr.status === 401) {
                setEntryStatus(entry, "error", "NOT AUTHENTICATED");
                showAuth();
            } else {
                setEntryStatus(entry, "error", data.error || "UPLOAD FAILED");
            }
        };

        xhr.onerror = function () {
            setEntryStatus(entry, "error", "NETWORK ERROR");
        };

        xhr.open("POST", "/api/upload");
        xhr.setRequestHeader("X-CSRF-Token", csrfToken);
        xhr.send(formData);
    }

    function pollStatus(filename, entry) {
        var elapsed = 0;
        var interval = setInterval(function () {
            elapsed += 2000;
            if (elapsed > 30000) {
                clearInterval(interval);
                setEntryStatus(entry, "error", "PROCESSING TIMEOUT");
                return;
            }
            fetch("/api/upload/status?filename=" + encodeURIComponent(filename))
                .then(function (r) { return r.json(); })
                .then(function (data) {
                    if (data.status === "processed") {
                        clearInterval(interval);
                        setEntryStatus(entry, "success", "PROCESSED");
                    } else if (data.status === "error") {
                        clearInterval(interval);
                        setEntryStatus(entry, "error", "PROCESSING FAILED");
                    }
                })
                .catch(function () {
                    clearInterval(interval);
                    setEntryStatus(entry, "error", "STATUS CHECK FAILED");
                });
        }, 2000);
    }

    function formatSize(bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(0) + " KB";
        return (bytes / (1024 * 1024)).toFixed(1) + " MB";
    }
})();
