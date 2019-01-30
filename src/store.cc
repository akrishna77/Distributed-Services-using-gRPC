#include "threadpool.h"

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "vendor.grpc.pb.h"
#include "store.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using vendor::BidReply;
using vendor::BidQuery;
using vendor::Vendor;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;
using store::Store;




class market {
public:

	struct ProductQueryResult {

		struct Bid {
			double price_;
			std::string vendor_id_;
		};
		std::vector<Bid> bids_;
	};

//	Reference: grpc examples greeter_aysnc_client.cc
	class VendorClient {
	public:
		explicit VendorClient(std::shared_ptr<Channel> channel)
				: stub_(Vendor::NewStub(channel)) {}

		bool get_prod_from_vendor(const std::string& server_addr, const std::string& product_name, ProductQueryResult& query_result) {

			BidQuery request;
			request.set_product_name(product_name);
			BidReply reply;
			ClientContext context;
			CompletionQueue cq;
			Status status;

			std::unique_ptr<ClientAsyncResponseReader<BidReply> > rpc(
					stub_->PrepareAsyncgetProductBid(&context, request, &cq));


			rpc->StartCall();
//			std::cout << "started rpc call\n";

			rpc->Finish(&reply, &status, (void*)1);
//			std::cout << "finished rpc call\n";
			void* got_tag;
			bool ok = false;
			GPR_ASSERT(cq.Next(&got_tag, &ok));
			GPR_ASSERT(got_tag == (void*)1);
			GPR_ASSERT(ok);

			if (status.ok()) {
			//	std::cout << "Replied correctly\n";
				ProductQueryResult::Bid bid;
				bid.price_ = reply.price();
				bid.vendor_id_ = reply.vendor_id();
				query_result.bids_.push_back(bid);
				return true;
			} else {
		//		std::cout << "Server did not reply rpc call\n";
				return false;
			}
		}

	private:
		std::unique_ptr<Vendor::Stub> stub_;
	};



	//	Reference: grpc examples greeter_aysnc_server.cc
	// Server class
	class customer_server final {
	public:
		~customer_server() {
			server_->Shutdown();
			cq_->Shutdown();
		}

		void Run(threadpool *pool, std::string server_address) {
			//std::string server_address("0.0.0.0:50050");

			ServerBuilder builder;
			builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
			builder.RegisterService(&service_);
			cq_ = builder.AddCompletionQueue();
			server_ = builder.BuildAndStart();
			std::cout << "Server listening on " << server_address << std::endl;

			HandleRpcs(pool);
		}

	private:
		class CallData {
		public:
			CallData(Store::AsyncService* service, ServerCompletionQueue* cq)
					: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
				Proceed();
			}

			void Proceed() {
				if (status_ == CREATE) {
//					std::cout << "Enter create"<< std::endl;
					status_ = PROCESS;

					service_->RequestgetProducts(&ctx_, &request_, &responder_, cq_, cq_, this);
				} else if (status_ == PROCESS) {
//					std::cout << "Enter Process"<< std::endl;
					new CallData(service_, cq_);

//					std::cout << "Filling ProductReply\n";
					Populate_ProductReply(request_, reply_);
					status_ = FINISH;
					responder_.Finish(reply_, Status::OK, this);
				} else {
					GPR_ASSERT(status_ == FINISH);
					delete this;
				}
			}

			// Populate product info object fetching from vendors
			void Populate_ProductReply(ProductQuery query, ProductReply reply) {
				std::ifstream vendor_addresses_file("vendor_addresses.txt");
				std::string line;
				std::vector<std::string> vendor_addresses;
				while (std::getline(vendor_addresses_file, line)) {
					vendor_addresses.push_back(line);
				}

				std::string product = query.product_name();
				market::ProductQueryResult query_result;
				for (int i = 0; i < vendor_addresses.size(); ++i) {
					market::VendorClient customer(grpc::CreateChannel(vendor_addresses[i], grpc::InsecureChannelCredentials()));
					bool status = customer.get_prod_from_vendor(vendor_addresses[i], product, query_result);
				}

				const auto bids = query_result.bids_;
				if (bids.size()) {
					//std::cout << "\n\nQuery id: " << 1 << ", product: " << product << " is -->  ";
					for (const auto bid : bids) {
					//	std::cout << "(price: " << bid.price_ << ", vendor_id: " << bid.vendor_id_ << ") ";
						store::ProductInfo *info = reply_.add_products();
						info->set_vendor_id(bid.vendor_id_);
						info->set_price(bid.price_);
					}
				} else {
					//std::cout << "\n\nQuery id: " << 1 << ", product: " << product << " -->  Didn't receive any bids";
				}
				//std::cout << std::endl;

				return;
			}

		private:
			Store::AsyncService* service_;
			ServerCompletionQueue* cq_;
			ServerContext ctx_;
			ProductQuery request_;
			ProductReply reply_;

			ServerAsyncResponseWriter<ProductReply> responder_;
			enum CallStatus { CREATE, PROCESS, FINISH };
			CallStatus status_;
		};

		void HandleRpcs(threadpool *pool) {
		//	std::cout << "RPC Handler called"<< std::endl;
			new CallData(&service_, cq_.get());
			void* tag;
			bool ok;
			while (true) {

				GPR_ASSERT(cq_->Next(&tag, &ok));
				GPR_ASSERT(ok);
				CallData* call = static_cast<CallData*>(tag);
				pool->enqueue([call](){call->Proceed();});
//				static_cast<CallData*>(tag)->Proceed();
			}
		}

		std::unique_ptr<ServerCompletionQueue> cq_;
		Store::AsyncService service_;
		std::unique_ptr<Server> server_;
	};


};


int main(int argc, char** argv) {

	std::size_t num_threads = atoi(argv[1]);
	std::string server_add = argv[2];
	//std::cout << server_add << std::endl;
	threadpool pool{num_threads};
	market::customer_server server;
	server.Run(&pool,server_add);
	return 0;
}

