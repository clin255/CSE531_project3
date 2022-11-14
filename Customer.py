import grpc
import bank_pb2
import bank_pb2_grpc
import time
import json

from utilities import get_operation, get_source_type_name, configure_logger, get_result_name
from concurrent import futures

logger = configure_logger("Customer")

class Customer:
    def __init__(self, id, bind_addresses, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches bind_address
        self.branches_bind_addresses = bind_addresses
        # iterate the processID of the branches the stub
        self.stubs = {}
        # local clock
        self.clock = 1
        # last write id, initial value is 0
        self.last_write_id = 0
        # last write branch id, initial value is 0
        self.last_write_branch = 0
        # write set, initial value is empty list
        self.write_set = []
        # customer balance
        self.balance = 0

    # TODO: students are expected to create the Customer stub
    def createStub(self, branch_id, branch_bind_address):
        """
        Create stub for each branch
        """
        stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(branch_bind_address))
        return stub

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            operation = get_operation(event["interface"])
            branch_id = event["dest"]
            if event["interface"] == "query":
                amount = 0
            else:
                amount = event["money"]
            logger.info(">" * 50 + "<" * 50)
            logger.info("Customer {} sending {} operation to branch {}, amount {}, last write id {}, last write branch {}".format(
                self.id, 
                event["interface"], 
                branch_id,
                amount,
                self.last_write_id,
                self.last_write_branch
                )
            )
            customer_request = bank_pb2.MsgDelivery_request(
                operation_type = operation,
                source_type = bank_pb2.Source.customer,
                id = self.id,
                amount = amount,
                clock = self.clock,
                last_write_id = self.last_write_id,
                last_write_branch = self.last_write_branch,
            )
            stub = self.createStub(branch_id, self.branches_bind_addresses[branch_id])
            response = stub.MsgDelivery(customer_request)
            self.recvMsg.append(response)
            logger.info("Customer {} has recevied the response from {} id {}, amount {}, clock {}, last_write_id {}".format(
                self.id, 
                get_source_type_name(response.source_type), 
                response.id,
                response.amount,
                response.clock,
                response.last_write_id
                )
            )
            # the last_write_id change means a write operation, so need to update write_set
            if response.last_write_id != self.last_write_id:
                self.last_write_branch = response.id
                self.last_write_id = response.last_write_id
            # save the customer balance and export the data later
            if event["interface"] == "query":
                self.balance = response.amount


def execute_customer_request(id, branch_bind_addresses, events):
    cust = Customer(id, branch_bind_addresses, events)
    cust.executeEvents()
    return {"id": cust.id, "balance": cust.balance}

