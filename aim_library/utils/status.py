from time import sleep
from os import environ
from threading import Thread
from aim_status_grpc.status_client import GRPCStatusClient

GRPC_STATUS_SERVER_HOST = environ.get('GRPC_STATUS_SERVER_HOST', 'documents_status.ticketai')
GRPC_STATUS_SERVER_PORT = environ.get('GRPC_STATUS_SERVER_PORT', '50052')

grpc_status_client = GRPCStatusClient(
    server_name = GRPC_STATUS_SERVER_HOST,
    port = GRPC_STATUS_SERVER_PORT
)

def _send_document_status(
    document_id: str,
    status: str,
    description: str,
    organization: str,
    meta: str
):
    for _ in range(3):
        try:
            grpc_status_client.update_document_status(
                document_id=document_id,
                status=status,
                description=description,
                organization=organization,
                meta=meta
            )
            break
        except Exception as e:
            sleep(1)
    else:
        print(f'ERROR: Document status update failed for {document_id}')
        print(f'INFO: Current config: (server_name={GRPC_STATUS_SERVER_HOST}, port={GRPC_STATUS_SERVER_PORT})')
        print(f'INFO: To change it set GRPC_STATUS_SERVER_HOST and GRPC_STATUS_SERVER_PORT environment variables')
    

def set_document_status(
    document_id: str,
    status: str,
    description: str,
    organization: str,
    meta: str
):
    thread = Thread(target=_send_document_status, args=(document_id, status, description, organization, meta))
    thread.start()
