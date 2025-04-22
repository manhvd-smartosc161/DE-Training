docker build --platform linux/amd64 \
    -t asia-southeast1-docker.pkg.dev/training-de-457603/docker-repo/hello-world:latest .

docker push asia-southeast1-docker.pkg.dev/training-de-457603/docker-repo/hello-world:latest

gcloud run deploy hello-world \
    --image asia-southeast1-docker.pkg.dev/training-de-457603/docker-repo/hello-world:latest \
    --platform managed \
    --region asia-southeast1 \
    --allow-unauthenticated