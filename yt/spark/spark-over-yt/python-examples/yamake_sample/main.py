import argparse
import logging
import spyt.submit as submit
import spyt.utils as utils


def run_job(proxy, discovery_path, client_version):
    logging.info("Running job")
    with submit.java_gateway() as gateway:
        logging.info("Gateway created")

        submission_client = submit.SparkSubmissionClient(gateway, proxy, discovery_path, client_version,
                                                         utils.default_user(), utils.default_token())
        logging.info("Submission client created")

        launcher = submission_client.new_launcher()
        launcher.set_app_resource("yt:///home/spark/examples/smoke_test.py")
        app_id = submission_client.submit(launcher)
        logging.info("Job submitted to cluster")

        status = submission_client.wait_final(app_id)
        logging.info(f"Final status: {status}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('proxy')
    parser.add_argument('discovery_path')
    parser.add_argument('client_version')
    args, _ = parser.parse_known_args()
    run_job(args.proxy, args.discovery_path, args.client_version)


if __name__ == '__main__':
    main()
