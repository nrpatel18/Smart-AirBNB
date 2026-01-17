# Deployment Guide - Smart AirBNB

## 🚀 Deploy to Render (Recommended - FREE & Easy)

### Prerequisites
- GitHub account
- This repository pushed to GitHub

### Step-by-Step Instructions

#### 1. Push Your Code to GitHub
```bash
git init
git add .
git commit -m "Initial commit - Ready for deployment"
git remote add origin https://github.com/YOUR_USERNAME/Smart-AirBNB.git
git push -u origin main
```

#### 2. Sign Up for Render
- Go to [render.com](https://render.com)
- Sign up with your GitHub account (it's free!)

#### 3. Deploy Using Blueprint (Easiest Method)
1. Click **"New +"** → **"Blueprint"**
2. Connect your GitHub repository
3. Render will detect `render.yaml` and set everything up automatically
4. Click **"Apply"** - Done! ✅

#### 4. Alternative: Manual Setup
If you prefer manual setup:

**Create PostgreSQL Database:**
1. Click **"New +"** → **"PostgreSQL"**
2. Name: `smart-airbnb-db`
3. Plan: **Free**
4. Click **"Create Database"**
5. Copy the **"Internal Database URL"**

**Create Web Service:**
1. Click **"New +"** → **"Web Service"**
2. Connect your GitHub repository
3. Settings:
   - **Name:** `smart-airbnb`
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app`
4. Add Environment Variable:
   - **Key:** `DATABASE_URL`
   - **Value:** Paste the Internal Database URL from step 5 above
5. Click **"Create Web Service"**

#### 5. Initialize Your Database
Once deployed, you need to initialize your database:
1. Go to your Web Service dashboard on Render
2. Click **"Shell"** (top right)
3. Run these commands:
   ```bash
   python
   >>> from app import init_db
   >>> init_db()
   >>> exit()
   ```

#### 6. Access Your App
- Your app will be available at: `https://smart-airbnb.onrender.com` (or similar)
- First load might take 30-60 seconds (free tier spins down when idle)

---

## 🔧 Other Deployment Options

### Deploy to Heroku ($5/month for PostgreSQL)
```bash
# Install Heroku CLI, then:
heroku create smart-airbnb
heroku addons:create heroku-postgresql:mini
git push heroku main
heroku run python -c "from app import init_db; init_db()"
heroku open
```

### Deploy to Railway (Free Tier Available)
1. Go to [railway.app](https://railway.app)
2. Click **"New Project"** → **"Deploy from GitHub"**
3. Select your repository
4. Add PostgreSQL from the dashboard
5. Railway auto-detects Flask and deploys

---

## ⚙️ Configuration Notes

### Environment Variables
The app automatically detects if it's running in production by checking for the `DATABASE_URL` environment variable:
- **Production:** Uses cloud PostgreSQL (from `DATABASE_URL`)
- **Local:** Uses localhost PostgreSQL (from `db_config.py`)

### What Changed for Deployment?
1. **`db_config.py`** - Now reads from environment variables in production
2. **`requirements.txt`** - Added `gunicorn` (production web server)
3. **`Procfile`** - Tells Render/Heroku how to start the app
4. **`render.yaml`** - Blueprint for automatic Render deployment

### Free Tier Limitations (Render)
- Database: 256MB storage, 90-day expiration (then you can create a new one)
- Web Service: Spins down after 15 min of inactivity (30-60s cold start)
- Perfect for demos, projects, and low-traffic apps!

---

## 🆘 Troubleshooting

**Database connection errors?**
- Make sure `DATABASE_URL` environment variable is set correctly
- Check that the database service is running

**App not loading?**
- First load takes 30-60 seconds on free tier
- Check logs in Render dashboard for errors

**Need to reset database?**
- In Render Shell: `python -c "from app import init_db; init_db()"`

---

## 📊 Monitoring
- View logs in real-time from the Render dashboard
- Free tier includes basic metrics and monitoring

---

**Questions?** Check [Render Docs](https://render.com/docs) or [Railway Docs](https://docs.railway.app)
