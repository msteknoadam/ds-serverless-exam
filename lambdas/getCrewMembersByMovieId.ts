import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
	DynamoDBDocumentClient,
	QueryCommand,
	QueryCommandInput,
	ScanCommand,
	ScanCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv();
const isValidQueryParams = ajv.compile(schema.definitions["MovieCrewMembersByMovieQueryParams"] || {});
const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
	try {
		console.log("Event: ", event);

		const pathParameters = event?.pathParameters;

		const movieId = pathParameters?.movieId ? parseInt(pathParameters.movieId) : undefined;
		const role = pathParameters?.role;

		if (!movieId) {
			return {
				statusCode: 400,
				headers: {
					"content-type": "application/json",
				},
				body: JSON.stringify({ Message: "Missing movieId parameter" }),
			};
		}
		if (!role) {
			return {
				statusCode: 400,
				headers: {
					"content-type": "application/json",
				},
				body: JSON.stringify({ Message: "Missing role parameter" }),
			};
		}

		const queryParams = event.queryStringParameters;

		if (!isValidQueryParams(queryParams)) {
			return {
				statusCode: 500,
				headers: {
					"content-type": "application/json",
				},
				body: JSON.stringify({
					message: `Incorrect type. Must match Query parameters schema`,
					schema: schema.definitions["MovieCrewMembersByMovieQueryParams"],
				}),
			};
		}

		const name = queryParams?.name;

		const commandInput: QueryCommandInput | ScanCommandInput = {
			TableName: process.env.TABLE_NAME,
			IndexName: "roleIx",
			KeyConditionExpression: "movieId = :m and begins_with(crewRole, :r) ",
			ExpressionAttributeValues: {
				":m": movieId,
				":r": role,
			},
		};

		let commandOutput;

		if (name) {
			commandInput.FilterExpression = "contains(names, :name)";
			commandInput.ExpressionAttributeValues!.name = name;

			commandOutput = await ddbDocClient.send(new ScanCommand(commandInput));
		} else {
			commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
		}

		return {
			statusCode: 200,
			headers: {
				"content-type": "application/json",
			},
			body: JSON.stringify({
				data: commandOutput.Items,
			}),
		};
	} catch (error: any) {
		console.log(JSON.stringify(error));
		return {
			statusCode: 500,
			headers: {
				"content-type": "application/json",
			},
			body: JSON.stringify({ error }),
		};
	}
};

function createDocumentClient() {
	const ddbClient = new DynamoDBClient({ region: process.env.REGION });
	const marshallOptions = {
		convertEmptyValues: true,
		removeUndefinedValues: true,
		convertClassInstanceToMap: true,
	};
	const unmarshallOptions = {
		wrapNumbers: false,
	};
	const translateConfig = { marshallOptions, unmarshallOptions };
	return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
