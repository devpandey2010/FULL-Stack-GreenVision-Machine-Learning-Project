# from fastapi import FastAPI,Request
# import uvicorn
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse,Response
# from datetime import datetime

# from src.forest.constant.application import APP_HOST
# from src.forest.pipeline.train_pipeline import TrainPipeline
# from src.forest.pipeline.prediction_pipeline import PredictionPipeline

# app=FastAPI()
# TEMPLATES=Jinja2Templates(directory='templates')

# app.add_middleware(CORSMiddleware,allow_origins=["*"],
#                    allow_credentials=True,
#                    allow_methods=["*"],
#                    allow_headers=["*"])

# @app.get("/",response_class=HTMLResponse)
# async def home(request:Request):
#      return "TEMPLATES.TemplateResponse("index.html", {"request": request})"

# @app.get("/train")
# @app.post("/train")
# async def trainRouteClient():
#     try:
#         train_pipeline=TrainPipeline()

#         train_pipeline.run_pipeline()

#         return HTMLResponse("<h1>Training successful !!<h1>")
    
#     except Exception as e:
#         return Response(f"Error Occured!{e}")
    
# @app.get("/predict")
# @app.post("/predict")
# async def predictRouteClient():
#     try:
#         prediction_pipeline=PredictionPipeline()

#         prediction_pipeline.initiate_prediction()

#         return HTMLResponse("<h1> Prediction successful and Predictions are stores in s3 bucket!!<h1>")
    
#     except Exception as e:
#         return Response(f"Error Occurred!{e}")
    

# if __name__=="__main__":
#     uvicorn.run(app,host="0.0.0.0",port=9000)

    
"""
GreenVision – FastAPI entry‑point
Save as app.py (or adjust the import path in uvicorn accordingly)
"""

import os
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

# ──────────────────────────────────────────────────────────────────────────────
#  Project‑specific imports
# ──────────────────────────────────────────────────────────────────────────────
from src.forest.pipeline.train_pipeline import TrainPipeline
from src.forest.pipeline.prediction_pipeline import PredictionPipeline

# ──────────────────────────────────────────────────────────────────────────────
#  FastAPI app and middleware
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="GreenVision API", version="1.0.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Templates and static assets
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# CORS (open in dev; tighten later in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
#  Routes
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, tags=["UI"])
async def home(request: Request) -> HTMLResponse:
    """
    Renders templates/index.html with a timestamp.
    """
    context = {
        "request": request,
        "timestamp": datetime.now().strftime("%Y‑%m‑%d %H:%M:%S"),
    }
    return templates.TemplateResponse("index.html", context)


@app.get("/train",tags=["ML Pipeline"])
@app.post("/train", tags=["ML pipeline"])
async def run_training():
    """
    Trigger the model‑training pipeline.
    """
    try:
        TrainPipeline().run_pipeline()
        return JSONResponse({"message": "Training completed successfully ✅"})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.get("/predict",tags=["ML pipeline"])
@app.post("/predict", tags=["ML pipeline"])
async def run_prediction():
    try:
        pipeline = PredictionPipeline()
        prediction_df = pipeline.initiate_prediction()  # returns a pandas DataFrame

        # Convert DataFrame to list of dicts for JSON serialization
        json_result = prediction_df.to_dict(orient="records")

        return JSONResponse(content={"predictions": json_result})

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
   


# ──────────────────────────────────────────────────────────────────────────────
#  Uvicorn entry‑point
# ──────────────────────────────────────────────────────────────────────────────
def start():
    """
    Allows `python -m app` or `uvicorn app:start` style execution.
    """
    uvicorn.run(
        "app:app",       # "module:variable"
        host="0.0.0.0",  # listen on all interfaces
        port=int(os.getenv("PORT", 8080)),
        reload=True,     # auto‑reload on code changes (great for dev)
    )


if __name__ == "__main__":
    start()

