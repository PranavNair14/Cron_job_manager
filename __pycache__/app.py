from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# ---------- In-memory Job Store ----------
jobs = []
job_counter = 1


# ---------- ROUTE: Home ----------
@app.route('/')
def home():
    return render_template('index.html', jobs=jobs)


# ---------- ROUTE: Create Job (CJM-1) ----------
@app.route('/create_job', methods=['POST'])
def create_job():
    global job_counter
    name = request.form.get('name')
    command = request.form.get('command')
    schedule = request.form.get('schedule')

    if name and command and schedule:
        new_job = {
            "id": job_counter,
            "name": name,
            "command": command,
            "schedule": schedule
        }
        jobs.append(new_job)
        job_counter += 1

    return redirect(url_for('home'))


# ---------- ROUTE: Edit Job (CJM-2) ----------
@app.route('/edit_job/<int:job_id>', methods=['GET', 'POST'])
def edit_job(job_id):
    global jobs
    job = next((job for job in jobs if job['id'] == job_id), None)

    if request.method == 'POST':
        if job:
            job['name'] = request.form.get('name')
            job['command'] = request.form.get('command')
            job['schedule'] = request.form.get('schedule')
        return redirect(url_for('home'))

    # GET request: load edit form
    return render_template('edit_job.html', job=job)


# ---------- ROUTE: Delete Job (CJM-3) ----------
@app.route('/delete_job/<int:job_id>', methods=['POST'])
def delete_job(job_id):
    global jobs
    jobs = [job for job in jobs if job['id'] != job_id]
    return redirect(url_for('home'))


# ---------- Run Flask App ----------
if __name__ == '__main__':
    app.run(debug=True)
