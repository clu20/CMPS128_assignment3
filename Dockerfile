FROM python:3
COPY . /assgn
WORKDIR /assgn
RUN pip install -r req.txt
ENTRYPOINT ["python"]
CMD ["app.py"]
EXPOSE 8080
